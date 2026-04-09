// 가장 최신의 GPR 지수를 가져와서 SQL 데이터베이스에 저장하는 스크립트입니다. 가장 최신 데이터를 가져옵니다.
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

async function fetchDailyGPR() {
  // ✅ 일일 데이터 전용 CSV URL (마테오 이아코비엘로 교수 제공)
  const url =
    "https://www.matteoiacoviello.com/ai_gpr_files/ai_gpr_data_daily.csv";

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

    if (rows.length === 0) throw new Error("데이터를 찾을 수 없습니다.");

    // 최신 데이터 1개 추출
    const latest = rows[rows.length - 1];

    const record = {
      category: "Economy",
      ai_gpr_index: parseFloat(latest["GPR_AI"]), // 👈 ai_gpr_index로 변경
      oil_disruptions: parseFloat(latest["GPR_OIL"] || 0),
      gpr_original: parseFloat(latest["GPR_AER"] || 0),
      non_oil_gpr: parseFloat(latest["GPR_NONOIL"] || 0),
      unit: "index",
      reference_date: latest["Date"],
      collected_at: new Date().toISOString(),
    };

    // ✅ indicator_daily_logs 테이블로 upsert
    const { data, error } = await supabase
      .from("indicator_daily_logs")
      .upsert(record, { onConflict: "reference_date" });

    if (error) throw error;
    console.log(`🚀 [Daily] 업데이트 성공: ${record.reference_date}`);
    console.log(
      `📊 지수: ${record.AI_GPR_Index} / 석유리스크: ${record.oil_disruptions}`,
    );
  } catch (error) {
    console.error("❌ 일일 데이터 수집 실패:", error.message);
  }
}

fetchDailyGPR();
