/**
 * [SEED SCRIPT] KOSIS 월간 과거 데이터 전수 수집
 * - 시작점: 최근 1000개월 (방법 B)
 * - 데이터가 없는 초기 구간은 3개 지표만 수집되고 나머지는 NULL로 유지됩니다.
 */
const axios = require("axios");
const { createClient } = require("@supabase/supabase-js");
const path = require("path");
require("dotenv").config({ path: path.resolve(__dirname, "../../../.env") });

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);
const KOSIS_API_KEY = process.env.KOSIS_API_KEY;

const MONTH_COUNT = 1000;

async function fetchKosisIntegratedMax(orgId, tblId, objL1, objL2, filterMap, useId = false) {
  const url = `https://kosis.kr/openapi/Param/statisticsParameterData.do?method=getList&apiKey=${KOSIS_API_KEY}&itmId=T+&objL1=${objL1}&objL2=${objL2}&format=json&jsonVD=Y&prdSe=M&newEstPrdCnt=${MONTH_COUNT}&orgId=${orgId}&tblId=${tblId}`;
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
        const yStr = row.PRD_DE.substring(0, 4);
        const mStr = row.PRD_DE.substring(4, 6);
        results.push({ reference_date: `${yStr}-${mStr}-01`, column_name: internalName, value: parseFloat(row.DT) });
      }
    });
    return results;
  } catch (error) {
    console.error(`❌ KOSIS API 실패:`, error.message);
    return [];
  }
}

async function seedKosisMonthlyMax() {
  console.log(`📦 [방법 B] KOSIS 월간 데이터 최대치 수집 시작...`);
  const idFilter = { "00": "cpi_total", "21": "core_cpi", "222": "cpi_no_energy" };
  const nameFilter = { "석유류": "cpi_petroleum", "전기가스수도": "cpi_utilities", "집세": "cpi_rent", "공공서비스": "cpi_public_service", "개인서비스": "cpi_private_service", "농축수산물": "cpi_agro", "공업제품": "cpi_industrial" };

  const rawData = [
    ...(await fetchKosisIntegratedMax("101", "DT_1J22002", "T10", "ALL", idFilter, true)),
    ...(await fetchKosisIntegratedMax("101", "DT_1J22112", "T10", "ALL", nameFilter, false))
  ];

  const dateMap = new Map();
  rawData.forEach(item => {
    if (!dateMap.has(item.reference_date)) {
      dateMap.set(item.reference_date, { reference_date: item.reference_date, collected_at: new Date().toISOString() });
    }
    dateMap.get(item.reference_date)[item.column_name] = item.value;
  });

  const columns = ["cpi_total", "core_cpi", "cpi_no_energy", "cpi_petroleum", "cpi_utilities", "cpi_rent", "cpi_public_service", "cpi_private_service", "cpi_agro", "cpi_industrial"];
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

  const BATCH_SIZE = 200;
  for (let i = 0; i < finalRows.length; i += BATCH_SIZE) {
    const batch = finalRows.slice(i, i + BATCH_SIZE);
    await supabase.from("indicator_kosis_monthly_logs").upsert(batch, { onConflict: "reference_date" });
  }
  console.log(`🎉 완료!`);
}
seedKosisMonthlyMax();
