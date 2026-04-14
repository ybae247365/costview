const axios = require("axios");
const { createClient } = require("@supabase/supabase-js");
const path = require("path");
require("dotenv").config({ path: path.resolve(__dirname, "../../../.env") });

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);
const KOSIS_API_KEY = process.env.KOSIS_API_KEY;

async function fetchKosisIntegrated(orgId, tblId, objL1, objL2, filterMap, useId = false) {
  const url = `https://kosis.kr/openapi/Param/statisticsParameterData.do?method=getList&apiKey=${KOSIS_API_KEY}&itmId=T+&objL1=${objL1}&objL2=${objL2}&format=json&jsonVD=Y&prdSe=M&newEstPrdCnt=3&orgId=${orgId}&tblId=${tblId}`;

  try {
    const response = await axios.get(url);
    const data = response.data;
    if (!Array.isArray(data)) return [];

    const cleanFilterMap = {};
    Object.keys(filterMap).forEach(key => {
      const cleanKey = useId ? key : key.replace(/[\s·,]/g, "");
      cleanFilterMap[cleanKey] = filterMap[key];
    });

    const results = [];
    data.forEach(row => {
      const kosisKey = useId ? row.C2 : row.C2_NM.replace(/[\s·,]/g, "");
      const internalName = cleanFilterMap[kosisKey];
      if (internalName) {
        const yyyy = row.PRD_DE.substring(0, 4);
        const mm = row.PRD_DE.substring(4, 6);
        results.push({
          reference_date: `${yyyy}-${mm}-01`,
          column_name: internalName,
          value: parseFloat(row.DT)
        });
      }
    });
    return results;
  } catch (error) {
    return [];
  }
}

async function run() {
  console.log("🚀 [Cost-Vue] KOSIS 데이터 병합 수집 시작...");

  const idFilter = {
    "00": "cpi_total",
    "21": "core_cpi",
    "222": "cpi_no_energy"
  };

  const nameFilter = {
    "석유류": "cpi_petroleum",
    "전기가스수도": "cpi_utilities",
    "집세": "cpi_rent",
    "공공서비스": "cpi_public_service",
    "개인서비스": "cpi_private_service",
    "농축수산물": "cpi_agro",
    "공업제품": "cpi_industrial"
  };

  const rawData = [
    ...(await fetchKosisIntegrated("101", "DT_1J22002", "T10", "ALL", idFilter, true)),
    ...(await fetchKosisIntegrated("101", "DT_1J22112", "T10", "ALL", nameFilter, false))
  ];

  if (rawData.length === 0) {
    console.warn("⚠️ 수집된 데이터가 없습니다.");
    return;
  }

  // 1. 날짜별 병합
  const dateMap = new Map();
  rawData.forEach(item => {
    if (!dateMap.has(item.reference_date)) {
      dateMap.set(item.reference_date, {
        reference_date: item.reference_date,
        collected_at: new Date().toISOString()
      });
    }
    dateMap.get(item.reference_date)[item.column_name] = item.value;
  });

  // 2. Forward Fill: DB에서 직전 유효값 로드
  const columns = [
    "cpi_total", "core_cpi", "cpi_no_energy", "cpi_petroleum",
    "cpi_utilities", "cpi_rent", "cpi_public_service", "cpi_private_service",
    "cpi_agro", "cpi_industrial"
  ];
  const sortedDates = [...dateMap.keys()].sort();
  const lastSeen = {};
  columns.forEach(col => { lastSeen[col] = null; });

  const oldest = sortedDates[0];
  const { data: prevRows } = await supabase
    .from("indicator_kosis_monthly_logs")
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
  const foundIndicators = [...new Set(rawData.map(d => d.column_name))];
  console.log("✅ 수집 지표:", foundIndicators.join(", "));

  const { error } = await supabase
    .from("indicator_kosis_monthly_logs")
    .upsert(finalRows, { onConflict: "reference_date" });

  if (!error) {
    console.log(`\n✨ KOSIS 데이터 ${finalRows.length}개월치 병합 저장 완료!`);
  } else {
    console.error("❌ DB 저장 실패:", error.message);
  }
}

run();
