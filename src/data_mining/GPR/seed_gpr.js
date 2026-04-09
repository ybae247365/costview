// 과거 1960년부터 오늘날까지의 데이터를 월간 단위로 가져와서 SQL 데이터베이스에 저장하는 스크립트입니다.
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

async function seedGPR() {
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

    // ✅ 변경된 컬럼명에 맞게 데이터 매핑
    const records = rows.map((row) => ({
      // source 컬럼 삭제됨
      category: "Economy",

      // value -> AI_GPR_Index로 변경
      AI_GPR_Index: parseFloat(row["GPR_AI"]),

      oil_disruptions: parseFloat(row["GPR_OIL"] || 0),
      gpr_original: parseFloat(row["GPR_AER"] || 0),
      non_oil_gpr: parseFloat(row["GPR_NONOIL"] || 0),

      unit: "index",
      reference_date: row["Date"],
      collected_at: new Date().toISOString(),
    }));

    // ✅ onConflict를 reference_date로 변경 (source가 없기 때문)
    const { error } = await supabase
      .from("indicator_logs")
      .upsert(records, { onConflict: "reference_date" });

    if (error) throw error;
    console.log(
      `✅ 구조 변경 반영 완료! 총 ${records.length}개의 데이터를 삽입했습니다.`,
    );
  } catch (error) {
    console.error("❌ Seed 작업 실패:", error.message);
  }
}

seedGPR();
