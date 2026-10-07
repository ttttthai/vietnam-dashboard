import sys; import os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from vn_deck import Deck
d = Deck(lang='vi')
d.title('Báo cáo kinh tế · ngân hàng', 'Lãi suất huy động: vì sao khó giảm', 'Sáu động lực và triển vọng 6 tháng', 'Cập nhật 7/10/2026')
d.hero('Tổng quan', 'Lãi suất tiết kiệm 12 tháng lên 5,9%, cao nhất từ 2023', '5,9', '%/năm',
       'Tăng 1,3 điểm % trong 12 tháng dù lãi suất điều hành không đổi.',
       chart={'type':'line','categories':['2021','2022','2023','2024','2025','2026'],
              'series':[{'name':'Tiết kiệm 12 tháng','values':[5.6,5.5,6.8,4.7,4.6,5.9]},{'name':'Tái cấp vốn','values':[4.0,6.0,4.5,4.5,4.5,4.5],'muted':True}],
              'number_format':'0.0'}, source='Nguồn: Vietcombank; NHNN. Số cuối tháng 9 mỗi năm.')
d.chart('Chương 1 · Tín dụng', 'Tín dụng tăng nhanh hơn huy động 6,7 điểm %',
        {'type':'bar','categories':['T1','T2','T3','T4','T5','T6','T7'],'series':[{'name':'Chênh lệch','values':[3.1,4.0,None,5.2,5.9,6.2,6.7]}],'number_format':'0.0','highlight':6,'labels':True},
        note=('Chênh lệch tín dụng − huy động đạt 6,7 điểm % (7/2026).','Tháng 3 chưa công bố — để trống, không nội suy.'), source='Nguồn: NHNN.')
d.two_charts('Chương 2 · Ngân sách','Thu nội địa chiếm 86% tổng thu năm 2025',
        {'type':'waffle','parts':[{'name':'Thu nội địa','value':86},{'name':'Xuất nhập khẩu','value':12},{'name':'Khác','value':2,'muted':True}]},
        {'type':'barh','categories':['Đất đai','FDI','DNNN','Ngoài quốc doanh'],'series':[{'name':'2024','values':[293.9,260.9,180.2,391.9]}],'number_format':'#,##0.0','labels':True},
        titles=('Cơ cấu thu 2025 (%)','Thu nội địa theo nguồn 2024 (nghìn tỷ)'), source='Nguồn: Bộ Tài chính.')
d.chart('Chương 3 · Chỉ số','Bốn con số chính', {'type':'bignumbers','tiles':[{'label':'CPI','value':'5,1','unit':'%','sub':'9/2026'},{'label':'Tín dụng','value':'11,6','unit':'%','sub':'YTD 30/9'},{'label':'LDR','value':'77','unit':'%','sub':'Q2/2026'}]}, source='Nguồn: NSO, NHNN.')
d.table('Phụ lục','Bảng số liệu', ['Năm','Thu','Chi','Bội chi'], [[2023,'1.770,8','1.936,9','291,6'],[2024,'2.057,5','2.148,5','323,3'],[2025,'2.650,1','2.401,5',None]], source='Nguồn: Bộ Tài chính.', number_cols=(1,2,3))
d.watch('Điều cần theo dõi','Ba mốc trong quý IV', [('13/10/2026','IMF WEO','Dự báo tăng trưởng mới.'),('20–31/10','KQKD quý 3 ngân hàng','Biên lãi ròng và nợ xấu.'),('1/12/2026','Trần LDR 95%','Tính 50% tiền gửi KBNN.')])
d.save(sys.argv[1] if len(sys.argv) > 1 else 'example.pptx')
