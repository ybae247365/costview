const axios = require("axios");
const { createClient } = require("@supabase/supabase-js");
const path = require("path");
require("dotenv").config({ path: path.resolve(__dirname, "../../../.env") });

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);
const ECOS_API_KEY = process.env.ECOS_API_KEY;

async function fetchEcosData(statCode, itemCode1, indicatorName) {
  const today = new Date();
  const past = new Date();
  past.setDate(today.getDate() - 7);

  const formatEcosDate = (date) => date.toISOString().split("T")[0].replace(/-/g, "");
  const startDate = formatEcosDate(past);
  const endDate = formatEcosDate(today);

  const url = `https://ecos.bok.or.kr/api/StatisticSearch/${ECOS_API_KEY}/json/kr/1/100/${statCode}/D/${startDate}/${endDate}/${itemCode1}`;

  try {
    const response = await axios.get(url);
    if (!response.data || !response.data.StatisticSearch || !response.data.StatisticSearch.row) {
      return [];
    }

    return response.data.StatisticSearch.row.map((row) => {
      const yyyy = row.TIME.substring(0, 4);
      const mm = row.TIME.substring(4, 6);
      const dd = row.TIME.substring(6, 8);
      return {
        reference_date: `${yyyy}-${mm}-${dd}`,
        name: indicatorName,
        value: parseFloat(row.DATA_VALUE)
      };
    });
  } catch (error) {
    console.error(`❌ ECOS API 호출 실패 [${indicatorName}]:`, error.message);
    return [];
  }
}

async function fetchEcosDailyLogs() {
  console.log("⏳ ECOS 일간 데이터(환율, 금리) 수집 및 병합 중...");

  const targets = [
    { stat: "731Y001", item: "0000001", name: "krw_usd_rate" },
    { stat: "817Y002", item: "010200000", name: "kr_bond_3y" }
  ];

  // 1. 날짜별로 모든 지표를 한 Row로 병합
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
    console.log("⚠️ 수집할 일간 데이터가 없습니다.");
    return;
  }

  // 2. 날짜 오름차순으로 정렬하여 Forward Fill 적용
  //    NULL(undefined)인 컬럼은 직전 유효한 날의 값으로 채워줍니다.
  const columns = targets.map(t => t.name);
  const sortedDates = [...dateMap.keys()].sort(); // "YYYY-MM-DD" 정렬

  const lastSeen = {}; // 각 컬럼의 가장 최근 유효값 기억
  columns.forEach(col => { lastSeen[col] = null; });

  // Supabase에서 이미 저장된 직전 데이터를 가져와서 lastSeen 초기값으로 활용
  const oldest = sortedDates[0];
  const { data: prevRows } = await supabase
    .from("indicator_ecos_daily_logs")
    .select("*")
    .lt("reference_date", oldest)
    .order("reference_date", { ascending: false })
    .limit(1);

  if (prevRows && prevRows.length > 0) {
    const prev = prevRows[0];
    columns.forEach(col => {
      if (prev[col] !== null && prev[col] !== undefined) {
        lastSeen[col] = prev[col];
      }
    });
    console.log(`ℹ️  직전 기준일(${prev.reference_date}) 마지막 유효값 로드 완료`);
  }

  // Forward Fill 순회
  sortedDates.forEach(date => {
    const row = dateMap.get(date);
    columns.forEach(col => {
      if (row[col] === undefined || row[col] === null || isNaN(row[col])) {
        // 오늘 데이터가 없으면 직전 값으로 채우고, 채웠다고 로그 남김
        if (lastSeen[col] !== null) {
          console.log(`↩️  [${date}] ${col} = NULL → 직전값 ${lastSeen[col]} 으로 대체`);
          row[col] = lastSeen[col];
        }
      } else {
        lastSeen[col] = row[col]; // 유효한 값이 있으면 기억
      }
    });
  });

  const finalRows = Array.from(dateMap.values());

  const { error } = await supabase
    .from("indicator_ecos_daily_logs")
    .upsert(finalRows, { onConflict: "reference_date" });

  if (error) {
    console.error("❌ ECOS 일간 데이터 저장 실패:", error.message);
  } else {
    console.log(`✅ ECOS 일간 데이터 ${finalRows.length}일치 병합 저장 완료!`);
  }
}

fetchEcosDailyLogs();
