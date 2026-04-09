// 이번달 GPR 지수와 과거 데이터를 모두 가져와서 SQL 데이터베이스에 저장하는 스크립트입니다.
const axios = require("axios");
const csv = require("csv-parser");
const { Readable } = require("stream");
const { createClient } = require("@supabase/supabase-js");
const path = require("path");
require("dotenv").config({ path: path.resolve(__dirname, "../.env") });

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY,
);

async function fetchAndStoreGPR() {
  const url =
    "https://www.matteoiacoviello.com/ai_gpr_files/ai_gpr_data_monthly.csv";

  try {
    const response = await axios.get(url, { responseType: "arraybuffer" });
    const rows = [];

    await new Promise((resolve, reject) => {
      Readable.from(response.data.toString())
        .pipe(csv())
        .on("data", (row) => rows.push(row))
        .on("end", resolve)
        .on("error", reject);
    });

    const latest = rows[rows.length - 1];

    // ✅ 수정된 데이터 구조: indicator_name 제거 및 직접 매핑
    const record = {
      source: "AI-GPR",
      category: "Economy",

      // 수치 매핑
      value: parseFloat(latest["GPR_AI"]),
      oil_disruptions: parseFloat(latest["GPR_OIL"] || 0), // 석유 공급망 차질 리스크
      gpr_original: parseFloat(latest["GPR_AER"] || 0), // 기존 키워드 기반 지수
      non_oil_gpr: parseFloat(latest["GPR_NONOIL"] || 0), // 비석유 관련 리스크

      unit: "index",
      reference_date: latest["Date"],
      collected_at: new Date().toISOString(),
    };

    // 🚀 Upsert 제약 조건 변경 (source, reference_date 조합)
    const { data, error } = await supabase
      .from("indicator_logs")
      .upsert(record, { onConflict: "source,reference_date" });

    if (error) throw error;
    console.log(`✅ 데이터 저장 완료 (${record.reference_date})`);
    console.log(`- 메인 지수: ${record.value}`);
    console.log(`- 석유 리스크: ${record.oil_disruptions}`);
  } catch (error) {
    console.error("❌ GPR 수집 실패:", error.message);
  }
}

fetchAndStoreGPR();
