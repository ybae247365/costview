const axios = require("axios");
const { createClient } = require("@supabase/supabase-js");
const path = require("path");
require("dotenv").config({ path: path.resolve(__dirname, "../../../.env") });

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);
const ECOS_API_KEY = process.env.ECOS_API_KEY;

async function fetchEcosData(statCode, itemCode1, indicatorName) {
  const today = new Date();
  const past = new Date();
  past.setMonth(today.getMonth() - 12);

  const formatEcosDate = (date) => {
    const yyyy = date.getFullYear();
    const mm = (date.getMonth() + 1).toString().padStart(2, "0");
    return `${yyyy}${mm}`;
  };

  const startDate = formatEcosDate(past);
  const endDate = formatEcosDate(today);

  const url = `https://ecos.bok.or.kr/api/StatisticSearch/${ECOS_API_KEY}/json/kr/1/100/${statCode}/M/${startDate}/${endDate}/${itemCode1}`;

  try {
    const response = await axios.get(url);
    if (!response.data || !response.data.StatisticSearch || !response.data.StatisticSearch.row) {
      return [];
    }
    return response.data.StatisticSearch.row.map(row => {
      const yyyy = row.TIME.substring(0, 4);
      const mm = row.TIME.substring(4, 6);
      return {
        reference_date: `${yyyy}-${mm}-01`,
        name: indicatorName,
        value: parseFloat(row.DATA_VALUE)
      };
    });
  } catch (error) {
    console.error(`❌ ECOS 월간 API 실패 [${indicatorName}]:`, error.message);
    return [];
  }
}

async function fetchEcosMonthlyLogs() {
  console.log("⏳ ECOS 월간 수입물가 데이터 병합 중...");

  const targets = [
    { stat: "401Y015", item: "201121AA", name: "import_price_crude_oil" },
    { stat: "401Y015", item: "201122AA", name: "import_price_natural_gas" }
  ];

  // 1. 날짜별 병합
  const dateMap = new Map();
  for (const target of targets) {
    const records = await fetchEcosData(target.stat, target.item, target.name);
    records.forEach(rec => {
      if (!dateMap.has(rec.reference_date)) {
        dateMap.set(rec.reference_date, {
          reference_date: rec.reference_date,
          collected_at: new Date().toISOString()
        });
      }
      dateMap.get(rec.reference_date)[rec.name] = rec.value;
    });
  }

  if (dateMap.size === 0) {
    console.log("⚠️ 수집할 월간 데이터가 없습니다.");
    return;
  }

  // 2. Forward Fill: DB에서 직전 유효값 로드
  const columns = targets.map(t => t.name);
  const sortedDates = [...dateMap.keys()].sort();
  const lastSeen = {};
  columns.forEach(col => { lastSeen[col] = null; });

  const oldest = sortedDates[0];
  const { data: prevRows } = await supabase
    .from("indicator_ecos_monthly_logs")
    .select("*")
    .lt("reference_date", oldest)
    .order("reference_date", { ascending: false })
    .limit(1);

  if (prevRows && prevRows.length > 0) {
    const prev = prevRows[0];
    columns.forEach(col => {
      if (prev[col] !== null && prev[col] !== undefined) lastSeen[col] = prev[col];
    });
    console.log(`ℹ️  직전 기준월(${prev.reference_date}) 마지막 유효값 로드 완료`);
  }

  // 3. NULL이면 직전값으로 대체
  sortedDates.forEach(date => {
    const row = dateMap.get(date);
    columns.forEach(col => {
      if (row[col] === undefined || row[col] === null || isNaN(row[col])) {
        if (lastSeen[col] !== null) {
          console.log(`↩️  [${date}] ${col} = NULL → 직전값 ${lastSeen[col]} 으로 대체`);
          row[col] = lastSeen[col];
        }
      } else {
        lastSeen[col] = row[col];
      }
    });
  });

  const finalRows = Array.from(dateMap.values());

  const { error } = await supabase
    .from("indicator_ecos_monthly_logs")
    .upsert(finalRows, { onConflict: "reference_date" });

  if (error) {
    console.error("❌ ECOS 월간 데이터 저장 실패:", error.message);
  } else {
    console.log(`✅ ECOS 월간 수입물가 ${finalRows.length}개월치 병합 저장 완료!`);
  }
}

fetchEcosMonthlyLogs();
