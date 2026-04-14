/**
 * [SEED SCRIPT] ECOS 월간 과거 데이터 전수 수집
 * - 시작점: 1960년 (최대치)
 * - 데이터가 없는 초기 구간(천연가스 등)은 NULL로 유지됩니다. (방법 B)
 */
const axios = require("axios");
const { createClient } = require("@supabase/supabase-js");
const path = require("path");
require("dotenv").config({ path: path.resolve(__dirname, "../../../.env") });

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);
const ECOS_API_KEY = process.env.ECOS_API_KEY;

const START_DATE = "196001";
const END_DATE = (() => {
  const d = new Date();
  const yyyy = d.getFullYear();
  const mm = (d.getMonth() + 1).toString().padStart(2, "0");
  return `${yyyy}${mm}`;
})();

async function fetchEcosMonthlyMaxRange(statCode, itemCode, indicatorName) {
  const url = `https://ecos.bok.or.kr/api/StatisticSearch/${ECOS_API_KEY}/json/kr/1/1000/${statCode}/M/${START_DATE}/${END_DATE}/${itemCode}`;
  try {
    const response = await axios.get(url);
    if (!response.data?.StatisticSearch?.row) return [];
    return response.data.StatisticSearch.row.map(row => {
      const yStr = row.TIME.substring(0, 4);
      const mStr = row.TIME.substring(4, 6);
      return { reference_date: `${yStr}-${mStr}-01`, name: indicatorName, value: parseFloat(row.DATA_VALUE) };
    });
  } catch (error) {
    console.error(`❌ [${indicatorName}] 실패:`, error.message);
    return [];
  }
}

async function seedEcosMonthlyMax() {
  console.log(`📦 [방법 B] ECOS 월간 데이터 최대치 수집 시작...`);
  const targets = [
    { stat: "401Y015", item: "201121AA", name: "import_price_crude_oil" },
    { stat: "401Y015", item: "201122AA", name: "import_price_natural_gas" }
  ];

  const dateMap = new Map();
  for (const target of targets) {
    const records = await fetchEcosMonthlyMaxRange(target.stat, target.item, target.name);
    records.forEach(rec => {
      if (!dateMap.has(rec.reference_date)) {
        dateMap.set(rec.reference_date, { reference_date: rec.reference_date, collected_at: new Date().toISOString() });
      }
      dateMap.get(rec.reference_date)[target.name] = rec.value;
    });
  }

  const columns = targets.map(t => t.name);
  const sortedDates = [...dateMap.keys()].sort();
  const lastSeen = {};
  columns.forEach(col => { lastSeen[col] = null; });

  const finalRows = sortedDates.map(date => {
    const row = dateMap.get(date);
    columns.forEach(col => {
      if (row[col] === undefined || row[col] === null || isNaN(row[col])) {
        if (lastSeen[col] !== null) row[col] = lastSeen[col];
      } else {
        lastSeen[col] = row[col];
      }
    });
    return row;
  });

  await supabase.from("indicator_ecos_monthly_logs").upsert(finalRows, { onConflict: "reference_date" });
  console.log(`🎉 완료!`);
}
seedEcosMonthlyMax();
