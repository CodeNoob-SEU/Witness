"""Minimal local-only OpenAI chat endpoint for the live context benchmark.

The server is intended to run inside an un-published Docker network. It logs
request sizes and timing, never message content. It is not a general-purpose
or internet-facing inference service.
"""

from __future__ import annotations

import argparse
import json
import time
import uuid
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, cast

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer


class LocalChatModel:
    def __init__(self, model: str, revision: str) -> None:
        self.model_id = model
        self.revision = revision
        self.tokenizer = AutoTokenizer.from_pretrained(
            model,
            revision=revision,
            trust_remote_code=False,
        )
        self.model = AutoModelForCausalLM.from_pretrained(
            model,
            revision=revision,
            torch_dtype="auto",
            device_map="auto",
            trust_remote_code=False,
        )
        self.model.eval()

    def complete(self, payload: dict[str, Any]) -> dict[str, Any]:
        if payload.get("model") != self.model_id:
            raise ValueError("request model does not match pinned model")
        messages = payload.get("messages")
        if not isinstance(messages, list):
            raise ValueError("messages must be a list")
        rendered = self.tokenizer.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=True,
        )
        encoded = self.tokenizer(rendered, return_tensors="pt")
        encoded = {key: value.to(self.model.device) for key, value in encoded.items()}
        requested_tokens = payload.get("max_completion_tokens", payload.get("max_tokens", 512))
        max_new_tokens = (
            requested_tokens
            if isinstance(requested_tokens, int)
            and not isinstance(requested_tokens, bool)
            and 1 <= requested_tokens <= 2_048
            else 512
        )
        started = time.perf_counter()
        with torch.inference_mode():
            generated = self.model.generate(
                **encoded,
                max_new_tokens=max_new_tokens,
                do_sample=False,
                pad_token_id=self.tokenizer.eos_token_id,
            )
        input_tokens = int(encoded["input_ids"].shape[-1])
        output_ids = generated[0, input_tokens:]
        output_tokens = int(output_ids.shape[-1])
        content = self.tokenizer.decode(output_ids, skip_special_tokens=True).strip()
        latency_ms = round((time.perf_counter() - started) * 1_000, 3)
        print(
            json.dumps(
                {
                    "event": "completion",
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "latency_ms": latency_ms,
                },
                sort_keys=True,
            ),
            flush=True,
        )
        return {
            "id": f"local-{uuid.uuid4().hex}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": f"{self.model_id}@{self.revision}",
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": content},
                    "finish_reason": "stop",
                }
            ],
            "usage": {
                "prompt_tokens": input_tokens,
                "completion_tokens": output_tokens,
                "total_tokens": input_tokens + output_tokens,
            },
        }


class Handler(BaseHTTPRequestHandler):
    server_version = "WitnessLocalChat/1"

    @property
    def chat_model(self) -> LocalChatModel:
        return cast("ModelServer", self.server).chat_model

    def log_message(self, format_string: str, *arguments: object) -> None:
        del format_string, arguments

    def _json(self, status: HTTPStatus, payload: object) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode()
        self.send_response(status.value)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path == "/health":
            self._json(HTTPStatus.OK, {"status": "ok"})
            return
        if self.path == "/v1/models":
            self._json(
                HTTPStatus.OK,
                {
                    "object": "list",
                    "data": [
                        {
                            "id": (
                                f"{self.chat_model.model_id}@"
                                f"{self.chat_model.revision}"
                            ),
                            "object": "model",
                            "owned_by": "local",
                        }
                    ],
                },
            )
            return
        self._json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def do_POST(self) -> None:
        if self.path != "/v1/chat/completions":
            self._json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > 4_000_000:
                raise ValueError("invalid content length")
            raw = self.rfile.read(length)
            payload = json.loads(raw)
            if not isinstance(payload, dict):
                raise ValueError("request must be an object")
            result = self.chat_model.complete(payload)
        except Exception as exc:
            print(
                json.dumps({"event": "request_failed", "error_type": type(exc).__name__}),
                flush=True,
            )
            self._json(
                HTTPStatus.BAD_REQUEST,
                {
                    "error": {
                        "message": "local inference request failed",
                        "type": type(exc).__name__,
                    }
                },
            )
            return
        self._json(HTTPStatus.OK, result)


class ModelServer(ThreadingHTTPServer):
    def __init__(self, address: tuple[str, int], chat_model: LocalChatModel) -> None:
        super().__init__(address, Handler)
        self.chat_model = chat_model


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    model = LocalChatModel(args.model, args.revision)
    print(
        json.dumps(
            {"event": "ready", "model": args.model, "revision": args.revision},
            sort_keys=True,
        ),
        flush=True,
    )
    ModelServer((args.host, args.port), model).serve_forever()


if __name__ == "__main__":
    main()
