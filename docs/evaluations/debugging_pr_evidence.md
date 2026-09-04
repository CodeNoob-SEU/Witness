# Runtime Debugging PR Evidence

Generated deterministically from journal events whose sequence/hash/digest consistency was verified; no model summarization was used.

- Run ID: `mcp-debug-c3b089a6245541e28309f171a0ede8bc`
- Journal head sequence: `23`
- Journal head hash: `e0209afa53d2f6486150e0b11d65eb0250c84a32948985394de03014dcdd1483`
- Evidence payload SHA-256: `6d8a469c5a5306c0c9c1b40e94e7a50ec7e209f0f003ca379fcbe0e33e1d0670`
- Report-generation model calls: `0`
- Replay debugger calls: `0`

## Reproduction

- Command argv: `[&quot;python3&quot;,&quot;examples/runtime_debug_demo/buggy_pricing.py&quot;,&quot;&lt;redacted:high-entropy&gt;&quot;]`
- Working directory: `.`

## Root-cause evidence (recorded facts)

Observed ZeroDivisionError at examples/runtime_debug_demo/buggy_pricing.py:31 in unit_price. Selected frame price_order at examples/runtime_debug_demo/buggy_pricing.py:42 recorded billable_items = [], feature_flags = {&#x27;bill_research&#x27;: False}, item_count = 0, order = {&#x27;order_id&#x27;: &#x27;order-golden-001&#x27;, &#x27;items&#x27;: [{...}]}, subtotal = 99.0.

### Observed failure

Observed `ZeroDivisionError` at `examples/runtime_debug_demo/buggy_pricing.py:31` in `unit_price`: float division by zero.

## Selected suspicious frame

Deterministic rule `workspace-user-frame-v1` selected frame index `1`: `examples/runtime_debug_demo/buggy_pricing.py:42` in `price_order`.
Selection reasons: `workspace_source, user_frame`.

## Captured frame variables

| Scope | Name | Type | Value | Flags |
| --- | --- | --- | --- | --- |
| locals | billable_items | list | [] | none |
| locals | feature_flags | dict | {&#x27;bill_research&#x27;: False} | none |
| locals | item_count | int | 0 | none |
| locals | order | dict | {&#x27;order_id&#x27;: &#x27;order-golden-001&#x27;, &#x27;items&#x27;: [{...}]} | none |
| locals | subtotal | float | 99.0 | none |

## Durable evidence timeline

| Seq | Event ID | Call key | Action | Outcome | Observation SHA-256 | Event hash |
| ---: | --- | --- | --- | --- | --- | --- |
| 2 | 44fc8caf-e32c-5b97-8b96-80a272217b16 | mcp:1 | launch | planned | — | 471b5f6cee9b5714c2423e09844517604281b8b95e35e2c5932e3b263afe351b |
| 3 | d3fbd9bd-067a-58a8-a808-f07a8c2df5dd | mcp:1 | launch | started | — | b134b47eb0db4b9fd1e821748bc3f992d997a955ecae774e907f8faf7a8a231d |
| 4 | 73895a15-9fad-558f-9a87-2bd1e39ea7d9 | mcp:1 | launch | completed | 5b45eb9df20d8c1b031fb04989de0b095adc73beb41b54f9fec17478fc0310af | 0254ecc97310db01d5c43197ff7f3602e70db85d28a2ad7ba2f71d2a40124775 |
| 5 | 468c0a30-1b2f-5a75-a5ba-3c94fe40f25d | mcp:2 | set_breakpoints | planned | — | 9261c394e5b3b9b9d397c69bf81bdf8a395f4829e7ba9439fba9963e03332b84 |
| 6 | bf74c44a-6a32-5906-ae4f-adc9aae2fbc3 | mcp:2 | set_breakpoints | started | — | 4d07ddbee444e3f8d016507c8954201c34c3f07d3732e08b5be017a219caacc4 |
| 7 | 25b3e2a4-27cd-5806-8c09-5b053d741bb6 | mcp:2 | set_breakpoints | completed | a0ac00271ad51bdeabce6cbdd6b0d1349c8f172bed349a1bcbe58796d03b3c4c | 49d23f985fde6d87b6ab3c46ee6b712fa9933250bb416b63d6402380e920b6f0 |
| 8 | 8cd16e2f-230d-5426-8940-a5b65bcd6a45 | mcp:3 | control | planned | — | 3b72688c22d195b73520bd4a8cd38440ac193193a5d4c6527a988363640af241 |
| 9 | adffca4f-d379-563f-a2c7-688f6a0ebe8d | mcp:3 | control | started | — | 417f13f0c1ef50f15bebf1d493a28532e381137eb938bc99562818353923649c |
| 10 | d3894dc9-cb64-55f1-879a-740f37cd4fdf | mcp:3 | control | completed | e34a9bc2ad238549ca2f4e37ded5497b6ea16cea5601471acfb2966e535357c3 | 91038412505252c59eaeec46280132bc5d787fe1b8fef832c1d060ddd63b8465 |
| 11 | 5f308b73-085b-562e-827e-5d5c1e50e062 | mcp:4 | stack | planned | — | fb9913d3d355ec62656fc2a574b2165e11cfa0c12187039733383f8ceb6dae06 |
| 12 | 3355e4a4-3466-5ef9-be0a-7e118f8ecd5e | mcp:4 | stack | started | — | 0b886ba0d440685f3b75e3f623734d7598ae4e1396cb2baee6b07d3b4005e8d2 |
| 13 | 4d730c8a-7b44-52ed-ae8e-94b798c74278 | mcp:4 | stack | completed | f4594dddbb50a3ab17cb42d4f86ef5065be5f3760fb08d0f632e19e880972323 | 31742aaf3beef77fdbe540a72579dfdc3733d80ed9f82913cb146ca62ff92c83 |
| 14 | ba63b151-942d-5654-84eb-bf6ff8f3ded7 | mcp:5 | select_frame | planned | — | 7580cab46ab108de612e7698f4e4e9d84e66d639726f369edf60e4a6f6529dff |
| 15 | b1be483e-a7fb-5919-9c1d-07d83520f120 | mcp:5 | select_frame | started | — | 9dda3d07bb75976f09124bf2a3a145111cd0c4e12368c728b3389cde52254ced |
| 16 | f82180f4-afaf-5c4b-bc07-6a53213f0684 | mcp:5 | select_frame | completed | 8e3859fc64e23271a970a331a154ae0355d7c902834f0288875e7750d3904660 | fae550c8b6158a2b6f194636bb4bd4bcfecb85e7ce19ca4bfd0895876e57c93f |
| 17 | 309f8cd7-2e92-5454-8496-a8d305c1c3b7 | mcp:6 | variables | planned | — | e21304bae9ae1e2135274193c4175d0e68ca053a82866c7be8bba09dce557fee |
| 18 | b10bbf5d-e407-53dd-b45a-136ec3f62b81 | mcp:6 | variables | started | — | 04b5b6a5a6dfcb7d8c9bc2b517263e6e442f969f3c31c2f01d78b19965cc7c7b |
| 19 | f67583a6-da97-567a-921d-da346b287aaa | mcp:6 | variables | completed | c261924aceafd8f2420d15b115fabed3c1a56daccffa84190427a1ab36ca1b33 | 8f7cd56d9616c7581e76e733a1fbe03754d3d5d5cdb601181880f2f9137bb521 |
| 20 | 6cf754ce-3f82-5af2-9302-27d893bfd99b | mcp:7 | stop | planned | — | 445014fbd781e7f576efaf292b49816afb389b7b0020b8b8d9c89760693d90e8 |
| 21 | 14e90bc2-ceac-574a-9b79-b7a8535df3d0 | mcp:7 | stop | started | — | 4d991995a0bd3c8540176e77510847033e19fb26f58f972d0a84a4ef0573574e |
| 22 | dae201bd-7bdf-517a-b70e-eda23aa9cfdf | mcp:7 | stop | completed | 306b3b41ae4d43c700e6880b68942d4ae2e79811c5745f09bb3851b18830db7d | 5f29ecd7bc4d9058203e6995eb5c4acdd038a45ee83155d7ba71d2e22ee43c64 |

## Debuggee termination

- Status: `terminated`
- Exit code: `None`
- Signal: `None`

## Evidence limitations

- This report states captured runtime facts only; it does not infer why the values arose.
- The unkeyed SHA-256 checks establish internal consistency, not provenance or authenticity; without a trusted external head, a writer able to rewrite the complete chain can recompute them.
