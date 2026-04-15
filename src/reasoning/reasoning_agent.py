import os
import json
from dotenv import load_dotenv

# 사용자 환경에 설치된 langchain 패키지에 따라 달라질 수 있으므로 기본 import 형태로 작성합니다.
try:
    from langchain_google_genai import ChatGoogleGenerativeAI
    from langchain_core.prompts import ChatPromptTemplate
    from langchain_core.output_parsers import JsonOutputParser
except ImportError:
    print("패키지 설치가 필요합니다: pip install langchain-google-genai langchain")

# 1. 환경변수 로드
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# 2. LLM 초기화 (기획안은 Claude 3.5지만, 현재 .env에는 Gemini만 있으므로 Gemini 적용)
try:
    llm = ChatGoogleGenerativeAI(
        model="gemini-1.5-pro-latest",
        google_api_key=GEMINI_API_KEY,
        temperature=0.2, # 팩트 기반이므로 창의성은 낮게 설정
    )
except Exception:
    llm = None

# 3. 프롬프트 엔지니어링 (System Prompt)
SYSTEM_PROMPT = """
당신은 최고의 경제/데이터 저널리스트이자 구조화 추론 엔진(Reasoning Engine)입니다. 
주어진 글로벌 뉴스(원인)와 원자재 지표를 바탕으로, 일반 대중의 '지갑(생활비)'에 어떤 타격이 갈지 2-step 인과관계로 추론하십시오.

[작업 지시사항]
1. 카테고리(category) 분류: Energy, Food, Utilities, Logistics 중 택 1
2. 타겟 속성(tags): 발생 지역과 대상 산업 (예: ["Middle East", "Energy"])
3. 제목(title): 30자 이내의 날카롭고 직관적인 인과관계 제목
4. 요약(description): "원인 -> 중간 파장 -> 최종 결과" 형태의 2-Step 요약 (60자 이내)
5. 타격 추정(metrics):
   - raw_shock_percent: 원자재의 예상 상승률 (0 ~ 500 내외의 정수값)
   - wallet_hit_percent: 실제 소비자 생활비에 전가될 비율 (보통 원자재 충격의 10% ~ 40% 내외. 정수값)
   - transmission_time_months: 도매에서 소매로 전이되는 시차 (1 ~ 6개월 선)

반드시 아래 JSON 스키마 형식에 맞춰서 응답하십시오. (```json 등 마크다운 블록 없이 순수 JSON만 반환할 것)

응답 예시:
{{
  "category": "Food",
  "tags": ["Black Sea", "Food"],
  "title": "흑해 곡물 수출 불안, 가공식품 가격 연쇄 상승",
  "description": "곡물 조달 차질이 밀가루 가격을 높여, 빵/외식비 부담으로 전이되었습니다.",
  "metrics": {{
    "raw_shock_percent": 65,
    "wallet_hit_percent": 19,
    "transmission_time_months": 2
  }}
}}
"""

prompt = ChatPromptTemplate.from_messages([
    ("system", SYSTEM_PROMPT),
    ("human", "다음 최신 뉴스와 지표를 분석해줘.\n\n[최신 뉴스]\n제목: {news_title}\n내용: {news_content}\n\n[거시 지표]\nWTI유: {wti_price}\nGPR지수: {gpr_index}")
])

# 4. 파이프라인(Chain) 체결
def analyze_impact(news_title: str, news_content: str, wti_price: float, gpr_index: float):
    if not llm:
        return {"error": "LLM 로드 실패"}
    
    chain = prompt | llm | JsonOutputParser()
    
    try:
        response = chain.invoke({
            "news_title": news_title,
            "news_content": news_content,
            "wti_price": wti_price,
            "gpr_index": gpr_index
        })
        return response
    except Exception as e:
        print(f"추론 실패: {e}")
        return None

# ==========================
# 테스트 실행부
# ==========================
if __name__ == "__main__":
    test_title = "이스라엘-이란 군사 충돌 국면 진입, 호르무즈 해협 봉쇄 우려 고조"
    test_content = "밤사이 중동 지역의 군사적 긴장이 최고조에 달하며, 전세계 물동량의 핵심인 호르무즈 해협의 봉쇄 가능성이 제기되었습니다. 이에 따라 글로벌 원유 공급망이 흔들리고 있습니다."
    
    print("\n🚀 [Reasoning Engine 작동 중...]\n")
    result = analyze_impact(
        news_title=test_title,
        news_content=test_content,
        wti_price=120.5,
        gpr_index=250.6
    )
    
    print("✅ [추론 결과 (JSON)]")
    print(json.dumps(result, indent=2, ensure_ascii=False))
