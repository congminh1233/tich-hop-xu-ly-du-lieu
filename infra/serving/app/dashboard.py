"""Read-only UI for imported TON_IoT experiments. All times displayed as UTC."""
import os
from datetime import datetime, timezone, timedelta
import pandas as pd
import psycopg
from psycopg.rows import dict_row
import streamlit as st

st.set_page_config(page_title='TON_IoT | Phân tích an ninh',page_icon='📊',layout='wide')

@st.cache_data(ttl=15)
def query(sql, params=()):
    with psycopg.connect(host=os.environ['PGHOST'],dbname=os.environ['PGDATABASE'],user=os.environ['PGUSER'],
                         password=os.environ['PGPASSWORD'],row_factory=dict_row,connect_timeout=10,
                         options='-c default_transaction_read_only=on -c statement_timeout=30000') as conn:
        return conn.execute(sql,params).fetchall()

st.title('TON_IoT · Phân tích an ninh IoT')
st.caption('IT5427 · Kết quả xử lý theo batch · Thời gian UTC')
try:
    experiments=query('SELECT * FROM serving.experiments ORDER BY loaded_at DESC,source_path')
except psycopg.Error:
    st.error('Chưa đọc được database. Kiểm tra dịch vụ PostgreSQL và chạy manage.py load trước.');st.stop()
if not experiments:
    st.info('Chưa có thí nghiệm. Chạy bước nạp dữ liệu trước.');st.stop()
with st.sidebar:
    st.header('Phạm vi phân tích')
    if st.button('Làm mới dữ liệu'):
        st.cache_data.clear();st.rerun()
    exp=st.selectbox('Thí nghiệm',experiments,format_func=lambda x:x['source_path']+' · '+x['experiment_id'][:8])
    st.caption('Mỗi thí nghiệm được hiển thị riêng; không cộng chồng các phiên bản.')
eid=exp['experiment_id']; summary=exp['summary']
options=query('SELECT DISTINCT subject_id,family,feature_group,split FROM serving.scores WHERE experiment_id=%s ORDER BY 1,2,3,4',(eid,))
if not options:
    st.warning('Thí nghiệm không có mô hình được fit hoặc không có dòng dự đoán.');st.json(summary);st.stop()
with st.sidebar:
    subject=st.selectbox('Host / cảm biến',sorted({r['subject_id'] for r in options}))
    families=sorted({r['family'] for r in options if r['subject_id']==subject})
    family=st.selectbox('Mô hình',families)
    groups=sorted({r['feature_group'] for r in options if r['subject_id']==subject and r['family']==family})
    group=st.selectbox('Nhóm đặc trưng',groups,index=groups.index('combined') if 'combined' in groups else 0)
    splits=sorted({r['split'] for r in options if r['subject_id']==subject and r['family']==family and r['feature_group']==group})
    split=st.selectbox('Tập dữ liệu',splits,index=splits.index('test') if 'test' in splits else 0)
base='experiment_id=%s AND subject_id=%s AND family=%s AND feature_group=%s AND split=%s'
params=(eid,subject,family,group,split)
bounds=query('SELECT min(window_start) lo,max(window_start) hi FROM serving.scores WHERE '+base,params)[0]
lo=pd.to_datetime(bounds['lo'],unit='s',utc=True).date();hi=pd.to_datetime(bounds['hi'],unit='s',utc=True).date()
with st.sidebar:
    dates=st.date_input('Khoảng ngày UTC',(lo,hi),min_value=lo,max_value=hi)
if len(dates)!=2:
    st.info('Chọn ngày bắt đầu và ngày kết thúc.');st.stop()
start=int(datetime.combine(dates[0],datetime.min.time(),tzinfo=timezone.utc).timestamp())
end=int((datetime.combine(dates[1],datetime.min.time(),tzinfo=timezone.utc)+timedelta(days=1)).timestamp())
where=base+' AND window_start >= %s AND window_start < %s';args=params+(start,end)
stats=query('SELECT count(*) n,coalesce(sum(prediction),0) alerts,count(label_observed) known FROM serving.scores WHERE '+where,args)[0]
a,b,c=st.columns(3)
a.metric('Cửa sổ được chấm điểm',f"{stats['n']:,}")
b.metric('Cửa sổ vượt ngưỡng',f"{stats['alerts']:,}")
c.metric('Tỷ lệ vượt ngưỡng',f"{100*stats['alerts']/stats['n']:.2f}%" if stats['n'] else '—')
st.caption('Cảnh báo là đầu ra của mô hình, không phải bằng chứng thiết bị đã bị xâm nhập. Điểm số không mặc định là xác suất.')
timeline, detail, metrics_tab, quality, provenance=st.tabs(['Diễn biến','Chi tiết cảnh báo','Đánh giá mô hình','Chất lượng & pipeline','Nguồn gốc kết quả'])
with timeline:
    hourly=query('SELECT (window_start/3600)*3600 epoch,count(*) windows,sum(prediction) alerts FROM serving.scores WHERE '+where+' GROUP BY 1 ORDER BY 1',args)
    if hourly:
        df=pd.DataFrame(hourly);df['Thời gian UTC']=pd.to_datetime(df.pop('epoch'),unit='s',utc=True)
        st.subheader('Số cửa sổ vượt ngưỡng theo giờ')
        st.bar_chart(df.set_index('Thời gian UTC')[['alerts']])
    points=query('SELECT window_start,score,threshold FROM serving.scores WHERE '+where+' ORDER BY window_start LIMIT 10000',args)
    if points:
        df=pd.DataFrame(points);df.index=pd.to_datetime(df.pop('window_start'),unit='s',utc=True)
        st.subheader('Điểm bất thường và ngưỡng')
        st.line_chart(df[['score','threshold']])
        if stats['n']>10000: st.caption('Biểu đồ điểm hiển thị 10.000 cửa sổ đầu tiên; thu hẹp khoảng ngày để xem phần còn lại.')
with detail:
    only_alert=st.checkbox('Chỉ hiện cửa sổ vượt ngưỡng',value=True)
    suffix=' AND prediction=1' if only_alert else ''
    rows=query('SELECT window_start,score,threshold,prediction,label_observed,payload::jsonb AS payload FROM serving.scores WHERE '+where+suffix+' ORDER BY window_start DESC LIMIT 1000',args)
    if rows:
        df=pd.DataFrame(rows);df.insert(0,'time_utc',pd.to_datetime(df['window_start'],unit='s',utc=True))
        st.dataframe(df.drop(columns=['payload']),use_container_width=True,hide_index=True)
        st.caption('Tối đa 1.000 dòng mới nhất trong bộ lọc; CSV xuất đúng phần đang hiển thị.')
        st.download_button('Tải CSV',df.drop(columns=['payload']).to_csv(index=False).encode('utf-8-sig'),'alerts_view.csv','text/csv')
        selected=st.selectbox('Xem dữ liệu giải thích gốc',range(len(rows)),format_func=lambda i:str(df.iloc[i]['time_utc']))
        st.json(rows[selected]['payload'])
    else:st.info('Không có dòng phù hợp với bộ lọc.')
with metrics_tab:
    st.caption('Metrics gốc của toàn bộ tập đã chọn, không áp dụng bộ lọc ngày. Nhãn được dùng để đánh giá; không suy ra từ dự đoán.')
    entries=summary.get('metrics',[])
    chosen=[]
    for r in entries:
        if r.get('entity_id',r.get('sensor_stream_id'))!=subject or r.get('split')!=split:continue
        if r.get('family',exp['script_version'])!=family:continue
        if r.get('target','label_observed')!='label_observed':continue
        chosen.append(r)
    if chosen:
        frame=pd.DataFrame(chosen)
        st.dataframe(frame,use_container_width=True,hide_index=True)
        gcol='group' if 'group' in frame.columns else 'model'
        if 'f1' in frame:
            frame['f1']=pd.to_numeric(frame['f1'],errors='coerce')
            st.bar_chart(frame.set_index(gcol)[['f1']])
        st.caption('So sánh các nhóm đặc trưng trong cùng thí nghiệm. Giá trị thiếu giữ nguyên; không thay bằng 0. Không cộng số dòng giữa các nhóm mô hình.')
    else:st.info('Không có metrics đánh giá cho lựa chọn này (ví dụ tập train).')
    if summary.get('integration_deltas'):
        with st.expander('Chênh lệch tích hợp từ báo cáo gốc'):st.dataframe(pd.DataFrame(summary['integration_deltas']),hide_index=True)
with quality:
    documents=query('SELECT source_path,body,imported_at FROM serving.documents ORDER BY source_path,imported_at DESC')
    matching=[d for d in documents if d['source_path']=='reports/quality_audit.json' and d['body'].get('snapshot_id')==exp['snapshot_id']]
    if matching:
        audit=matching[0]['body']
        st.subheader('Bản ghi cách ly theo nguồn')
        st.dataframe(pd.DataFrame(audit.get('quarantine',[])),hide_index=True)
        st.subheader('Kiểm tra Gold');st.json(audit.get('gold',{}))
        with st.expander('Toàn bộ báo cáo chất lượng'):st.json(audit)
    else:st.info('Chưa có quality_audit khớp snapshot của thí nghiệm này.')
    proofs=[d for d in documents if d['source_path'].startswith('reports/hdfs/') and d['source_path'].endswith('/summary.json')]
    st.subheader('Bằng chứng vận hành đã nhập')
    st.caption('Đây là báo cáo của các lượt chạy trước, không phải giám sát trực tiếp. Snapshot thí nghiệm giữ nguyên ID nguồn; không tự đổi sang ID HDFS.')
    for d in proofs:
        with st.expander(d['source_path']):st.json(d['body'])
with provenance:
    st.write('Snapshot nguồn:',exp['snapshot_id'])
    st.write('Phiên bản chương trình:',exp['script_version'])
    st.code(eid,language=None)
    for warning in summary.get('warnings',[]):st.warning(warning)
    with st.expander('Summary gốc'):st.json(summary)
    with st.expander('Checksum đầu vào serving'):st.json(exp['manifest'])
    st.caption('Dashboard đọc các kết quả đã tính. Phiên bản này không huấn luyện lại mô hình và chưa tự cập nhật qua DAG Airflow.')
