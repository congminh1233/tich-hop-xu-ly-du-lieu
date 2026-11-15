"""Small offline milestone dashboard. Reads committed snapshot metadata only."""
import html
from pathlib import Path
from .common import read_json
from .ingest import manifests


def table(headers, rows):
    return "<table><thead><tr>" + "".join(f"<th>{html.escape(str(h))}</th>" for h in headers) + "</tr></thead><tbody>" + "".join("<tr>" + "".join(f"<td>{html.escape(str(v))}</td>" for v in row) + "</tr>" for row in rows) + "</tbody></table>"


def create_report(lake, output, snapshot_id=None):
    lake = Path(lake)
    snapshot_id = snapshot_id or read_json(lake / "latest.json")["snapshot_id"]
    meta = read_json(lake / "snapshots" / snapshot_id / "snapshot.json")
    included = set(meta["spec"]["input_manifest_ids"])
    catalog = [m for _, m in manifests(lake) if m["source_file_id"] in included]
    total = sum(x["input_rows"] for x in meta["source_accounting"])
    rejected = sum(x["quarantined_source_rows"] for x in meta["source_accounting"])
    counts = {}
    for m in catalog:
        for label, n in m["attack_types"].items():
            counts[label] = counts.get(label, 0) + n
    # Descriptive ground-truth distribution; never a prediction or performance metric.
    bars = "".join(f'<div class="bar"><span>{html.escape(k)}</span><i style="width:{max(1, 65*v/max(counts.values())):.2f}%"></i><b>{v:,}</b></div>' for k, v in sorted(counts.items(), key=lambda x: -x[1])) if counts else ""
    rows = [(x["file_name"], f'{x["input_rows"]:,}', f'{x["accepted_source_rows"]:,}', f'{x["quarantined_source_rows"]:,}') for x in meta["source_accounting"]]
    coverage = [(x["entity_id"], x["has_network"], x["has_os"], x["has_cpu"], x["count"]) for x in meta["host_coverage"]]
    doc = f'''<!doctype html><html lang="vi"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>TON IoT · Chất lượng tích hợp</title>
<style>body{{margin:0;background:#f4f7fa;color:#142332;font:16px/1.6 system-ui,sans-serif}}main{{max-width:1120px;margin:auto;padding:40px 24px}}small{{color:#49677f}}h1{{font-size:34px;margin:8px 0}}h2{{margin-top:32px}}.stats{{display:flex;gap:20px;flex-wrap:wrap}}.stats div{{flex:1;background:white;padding:22px;border:1px solid #dce5ee;min-width:190px}}strong{{display:block;font-size:30px}}section{{background:white;padding:24px;margin-top:22px;border:1px solid #dce5ee}}table{{border-collapse:collapse;width:100%;font-size:14px}}th,td{{padding:10px;border:1px solid #dce5ee;text-align:left;overflow-wrap:anywhere}}th{{background:#eaf1f8}}.scroll{{overflow:auto}}.bar{{display:flex;gap:12px;align-items:center;margin:9px 0;font-size:14px}}.bar span{{width:100px}}.bar i{{height:15px;background:#2479ab}}.bar b{{font-weight:500}}code{{overflow-wrap:anywhere}}details{{margin-top:20px}}a{{color:#075e95}}</style>
<main><small>IT5427 · TÍCH HỢP VÀ XỬ LÝ DỮ LIỆU LỚN · MỐC 1</small><h1>Chất lượng dữ liệu TON IoT</h1>
<p>Snapshot <code>{html.escape(snapshot_id)}</code>. Phạm vi đầu vào: <b>{'đủ các tệp đã khai báo' if meta['input_scope_complete'] else 'còn thiếu tệp'}</b>. Đây là thống kê dữ liệu và chất lượng tích hợp; detector chưa được triển khai.</p>
<div class="stats"><div>Số dòng đầu vào<strong>{total:,}</strong></div><div>Số tệp đã xử lý<strong>{len(catalog)}</strong></div><div>Dòng cách ly<strong>{rejected:,}</strong></div></div>
<section><h2>Hạch toán trước khi chuyển sang dạng dài</h2><div class="scroll">{table(['Tệp','Đầu vào','Chấp nhận','Cách ly'],rows)}</div></section>
<section><h2>Phân bố nhãn gốc</h2><p>Các nhãn dưới đây do dataset cung cấp, không phải kết quả dự đoán.</p>{bars}</section>
<section><h2>Coverage theo host và cửa sổ</h2><p>Thiếu mẫu trong một cửa sổ khác với nguồn chưa được nạp. Host có dữ liệu OS nhưng CPU lỗi vẫn được phân biệt bằng cột CPU.</p><div class="scroll">{table(['Host','Có Network','Có OS','Có CPU hợp lệ','Số cửa sổ'],coverage)}</div></section>
<details><summary>Các tệp còn thiếu và giới hạn</summary><p>{html.escape(', '.join(meta['missing_expected_files']) or 'Không thiếu trong phạm vi khai báo.')}</p><p>Chỉ dùng IP Windows từ topology công bố. Telemetry giữ định danh luồng riêng. Không suy diễn quan hệ nhân quả, chiến dịch tấn công hay độ chính xác detector từ biểu đồ này.</p></details>
<p><small>Thời gian chạy Spark: {meta['elapsed_seconds']:.2f} giây · {html.escape(meta['execution_master'])} · cửa sổ {meta['spec']['window_seconds']} giây</small></p></main></html>'''
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(doc, encoding="utf-8")
    return str(path.resolve())
