# Translation rules (Vietnamese → English) for a Vietnam economic dashboard
- Output concise dashboard/financial-report English (FT / Bloomberg style). Sentence case. Keep it short.
- Keep unchanged: tickers (VCB, POW…), company/bank names (Vietcombank, PV Power…), province/city names (Hà Nội, TP. Hồ Chí Minh…), people's names, document numbers (57-NQ/TW, NQ 202/2025/QH15, QĐ 768/QĐ-TTg), URLs, numbers, %, symbols (▲ ▼ ✓ ✗ ◐ ⚠ →), HTML entities, emoji, and month labels like "T9/26" or "T10/24" (another component converts them).
- Placeholders {0} {1} … must appear in the English exactly once each (order may change).
- Glossary: tỷ / tỷ đồng / tỷ VND → bn VND (or "bn" when unit obvious); nghìn tỷ → trn VND; triệu → mn; tr (triệu) → mn; đồng/đ → VND; cp → shares;
  CPI giữ nguyên; YoY/MoM giữ nguyên; "so với T12 năm trước" / YTD → "vs Dec (prior year)"; lũy kế → cumulative; bình quân/TB → avg; đ.% / điểm % → pp;
  NHNN → SBV; Big4 → Big 4; NHTMCP → JSC banks; TPCP → government bonds (G-bonds); trái phiếu → bonds; lợi suất → yield; trúng thầu → auction (winning) yield;
  NSNN → state budget; dự toán → budget plan; giải ngân → disbursement; đầu tư công → public investment; nợ công → public debt; bội chi → deficit;
  tín dụng → credit; huy động → deposits/funding; dư nợ → loans outstanding; VCSH → equity; LNST → net profit; LNTT → pre-tax profit; biên LN gộp → gross margin;
  Bộ Chính trị → Politburo; Ban Chấp hành Trung ương → Party Central Committee; Quốc hội → National Assembly; Chính phủ → Government; Thủ tướng/TTg → Prime Minister;
  Nghị quyết → Resolution; Chỉ thị → Directive; Kết luận → Conclusion; Quyết định → Decision; Cục Thống kê/GSO/NSO → NSO; Bộ Tài chính → Ministry of Finance (MoF);
  tỉnh → province; vùng → region; cả nước → nationwide; Kinh tế → Economy; Tiền tệ → Monetary; Chính sách tài khoá → Fiscal policy; Lạm phát → Inflation;
  Xã hội → Society; Hệ thống Ngân hàng → Banking system; Đầu tư → Investing; Định hướng và tầm nhìn → Strategy & vision;
  dự báo → forecast; mô hình/mô hình hoá → modelled; ước tính → estimate; nhập tay → manual entry; Thiếu mẫu → Insufficient sample; thắng → win; Mua → Buy; Khả quan → Outperform;
  Chi tiết → Details; Thu gọn → Collapse; Cập nhật → Update; Nhật ký → Log; Quét → Scan; Phân tích → Analyse; Thêm → Add.
- Do NOT add explanations. Return only the JSON.
