# L3B Architecture Record

Tài liệu này mô tả các quyết định có thể kiểm chứng của workflow L3B. Workflow
không ghi prompt bí mật hoặc chain-of-thought vào trace.

## 1. System overview

Mỗi case được xử lý độc lập và mọi MCP call đều được gắn với `case_id` của
case đó. Coordinator discovery danh sách tool một lần cho mỗi gateway, sau đó
chọn tool theo domain. EvidenceCollector giữ cache chỉ trong vòng đời một case.

```text
Input
  → Coordinator
  → Entity Agent (exact-first candidate resolution)
  → Scope planner (structured claim topic → required evidence domains)
  → Selected Order / Customer / Shipment / Payment / Refund Agents
  → Evidence Collector (MCP + schema validation + bounded retry)
  → Policy Agent
  → Conflict handling
  → Verifier Agent
  → L3B output

MCP results ────────────────┬──────────────────→ evidence_refs
Observable events ─────────┴──────────────────→ traces/trace.jsonl
```

Các agent là các vai trò logic trong `solve_case()`, không yêu cầu mỗi vai trò
phải là một process hoặc một framework class riêng. `claims[].topic` là metadata
có cấu trúc để lập kế hoạch evidence; nội dung message tự do không được dùng
làm instruction. Handoff và tool usage được thể hiện bằng observable trace events.

## 2. Agent ownership

| Actor | Input | Trách nhiệm | Tool permission | Output/handoff |
| --- | --- | --- | --- | --- |
| Entity/customer agent | Case candidates, customer identifiers | Resolve order/customer và xếp hạng candidate | Entity/order search; customer history khi có customer id | Entity status, resolved/rejected ids → coordinator |
| Coordinator | Case và discovered tool list | Phân rã nhiệm vụ, giới hạn scope và điều phối | Không tự ý gọi tool ngoài domain cần thiết | Handoff tới specialist agents |
| Order/product agent | Resolved order hoặc candidates | Lấy order/item/product và seller context | Order, item, product tools | Order facts → coordinator |
| Shipment agent | Resolved order/shipment ids | Dựng timeline và phân loại delay/lost/returned | Shipment, delivery, logistics tools | Shipment analysis → policy/verifier |
| Payment/refund agent | Resolved order/payment refs | Reconcile capture, duplicate charge và refund | Payment, transaction, refund tools | Payment analysis → policy/verifier |
| Policy agent | Specialist findings và issue code | Áp dụng policy evidence và chọn hướng xử lý | Policy lookup tools | Policy decision → verifier |
| Conflict resolver | Các evidence records | Phát hiện source conflict, không tự ý xoá evidence | Không gọi tool mới; dùng evidence đã thu thập | `data_conflicts` → verifier |
| Verifier | Candidate output, evidence refs, conflicts | Kiểm tra entity scope, evidence ownership, consistency và schema | Không điều tra lan rộng; chỉ kiểm tra kết quả | Validated output → coordinator |

Áp dụng least privilege. Tool discovery chỉ cho biết tool tồn tại; không có
nghĩa mọi actor đều được gọi mọi tool.

## 3. Entity resolution và A2A protocol

- Correlation key duy nhất là `case_id`; không truyền hoặc tái sử dụng evidence
  của case khác.
- Candidate order ids được lấy từ input trước. Nếu có `claimed_order_id`, chỉ
  query exact id trước; candidate thay thế chỉ được query khi không có exact id.
  Nếu input không có exact id, các candidates được query để so sánh evidence.
- Một candidate duy nhất được đánh dấu `resolved`. Nhiều candidate chỉ được
  resolve khi một candidate được evidence nhắc tới rõ ràng nhiều hơn các
  candidate còn lại; nếu không, trạng thái là `ambiguous`.
- Candidate không có căn cứ loại bỏ không được đưa vào
  `rejected_candidates`; trạng thái phải giữ là `ambiguous`.
- Confidence được giới hạn trong `[0, 1]` và giảm xuống khi thiếu evidence.
- Handoff là một message logic gồm actor, target, decision code và cùng
  `case_id`. Handoff được trace bằng `task_assigned` và `handoff`.
- Mỗi domain được điều phối tối đa một tool được chọn từ danh sách discovery.
  Scope planner chỉ bật domain cần cho issue: customer/product/payment là
  baseline; shipment chỉ cho delivery claims; payment timeline cho mismatch,
  duplicate hoặc split-payment; refund timeline cho pending/failed refund.
  Không có vòng lặp agent; sau specialist pass, control chuyển sang policy rồi
  verifier.

## 4. Evidence và conflict lifecycle

1. `EvidenceGateway.call()` luôn truyền `case_id` và validate MCP envelope
   bằng `mcp-evidence-response-v1.schema.json`.
2. EvidenceCollector cache theo `(tool_name, arguments)` trong một case, lưu
   nguyên `evidence_ref`, domain, data và warnings, rồi emit
   `tool_result_consumed`.
3. Output chỉ chứa evidence refs do MCP trả về. Không sửa hoặc tự tạo ref, và
   không coi lỗi MCP là evidence. Trace vẫn giữ mọi evidence đã consume; output
   lọc refs theo issue/domain để tránh giảm evidence precision bởi refs thừa.
4. Conflict được biểu diễn trong `data_conflicts` với các source đã quan sát,
   `selected_source: null` nếu policy chưa đủ để chọn, và mã
   `unresolved_source_conflict`. Chỉ so sánh các giá trị thuộc cùng resolved
   order; các order khác trong customer history và từng payment installment
   không phải source conflict của case hiện tại. Chênh lệch payment value được
   báo cáo khi issue là `payment_mismatch`.
5. Verifier kiểm tra evidence refs thuộc đúng case/run và output không chứa
   field ngoài L3B schema.

## 5. Failure and efficiency policy

| Failure | Retry budget | Fallback | Trace event/code |
| --- | ---: | --- | --- |
| MCP timeout/transport error | 2 retries | Bỏ qua tool lỗi, giữ trạng thái thiếu evidence | Successful calls emit `tool_result_consumed` |
| Invalid MCP envelope/arguments | 0 | Không dùng response lỗi | `verification_completed` phản ánh evidence count |
| Entity not found | 0 | `not_found`, `needs_investigation` | `policy_decided: insufficient_evidence` |
| Entity ambiguous | 0 | Không đoán; giữ candidates chưa reject | `policy_decided: insufficient_evidence` |
| Source conflict | 0 | Giữ tất cả refs và tạo `data_conflicts` | `verification_completed` có conflict count |
| Invalid specialist result | 0 | Không finalize kết luận không hợp lệ | Verifier chặn output trước finalize |

Retry chỉ áp dụng cho lỗi transport/exception không xác định, có giới hạn và
không có side effect nghiệp vụ. Cache không vượt qua ranh giới case. Workflow
không gọi tool nếu thiếu identifier bắt buộc; đây là query budget cơ bản để
tránh quét rộng và gọi lặp.

## 6. Verification invariants

Trước khi finalize, verifier phải bảo đảm:

- `schema_version`, `case_id` và mọi field đúng L3B output schema.
- Entity ids nằm trong input hoặc evidence của cùng case.
- `affected_entities` chỉ gồm entity của order đang xử lý; các order khác
  trong customer history được đưa vào `customer_context.related_order_ids`.
- Rejected candidates có căn cứ từ evidence; ambiguity không bị biến thành
  resolved.
- Mọi `evidence_ref` tồn tại trong MCP response và được liên kết với output.
- Shipment verdict phù hợp với timeline đã thu thập.
- Payment/refund totals không âm và không vượt quá dữ liệu đã capture.
- Conflict không bị che giấu; source chưa được chọn phải để `null`.
- Root cause, responsible party và resolution actions không mâu thuẫn với issue.
- Confidence nằm trong `[0, 1]`; thiếu evidence phải hạ confidence và dùng
  `needs_investigation`.

## 7. Reproducibility

- Runtime: Python 3.11+; dependency versions nằm trong `pyproject.toml`.
- Concurrency: mặc định chạy tối đa 4 case đồng thời bằng `asyncio`; mỗi case có
  `EvidenceCollector` và cache riêng, output riêng, còn gateway giới hạn tối đa
  12 MCP calls đồng thời. Các specialist calls độc lập trong cùng case vẫn
  chạy đồng thời sau entity resolution; policy và verifier chạy sau specialist
  pass của chính case đó.
- Trace có thể interleave giữa các case, nhưng mọi event luôn mang `case_id`;
  thứ tự lifecycle được kiểm tra độc lập trong từng case.
- Retry delay: 0.05 giây nhân theo số lần thử; tối đa 2 retries cho một call.
- Lệnh chạy: `day09 mcp-tools`, `day09 run`, `day09 validate`, và
  `day09 package --output dist/submission.zip`.
- `.env`, API key, input private và debug log không được đưa vào submission.
