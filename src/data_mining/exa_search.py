import os
from dotenv import load_dotenv
from exa_py import Exa
from typing import List, Dict, Any

load_dotenv()

# EXA API 키 체크
if not os.getenv("EXA_API_KEY"):
    raise ValueError("EXA_API_KEY가 환경 변수에 설정되어 있지 않습니다.")

exa_client = Exa(api_key=os.environ["EXA_API_KEY"])

def fetch_economic_news(semantic_query: str, num_results: int = 5) -> List[Dict[str, Any]]:
    """
    경제적 맥락(시맨틱 쿼리)을 바탕으로 Exa를 통해 다중 소스의 뉴스를 수집합니다.
    (교차 검증을 위한 기초 데이터 수집)
    
    Args:
        semantic_query (str): "브라질 커피 농가 가뭄 피해 상황" 등 구체적인 맥락형 질문
        num_results (int): 교차 검증을 위해 수집할 최소 기사 개수
        
    Returns:
        List[Dict]: 수집된 기사의 제목, URL, 본문 하이라이트가 포함된 딕셔너리 리스트
    """
    try:
        # Cost-Vue PRD 요구사항 REQ-01(시맨틱), REQ-02(다중 소스 교차 검증) 적용
        response = exa_client.search_and_contents(
            query=semantic_query,
            num_results=num_results,
            use_autoprompt=True, # Exa 내부적으로 쿼리를 최적화
            text={"max_characters": 2000}, # 본문 내용 추출 한도
            highlights={"num_sentences": 5} # 핵심 문장 요약본 (노이즈 필터링 보조용 REQ-04)
        )
        
        extracted_news = []
        for result in response.results:
            news_data = {
                "title": result.title,
                "url": result.url,
                "text": result.text, # 전체 텍스트 중 앞 2000자
                "highlights": result.highlights,
                "id": result.id
            }
            extracted_news.append(news_data)
            
        return extracted_news

    except Exception as e:
        print(f"❌ 데이터 수집 중 오류 발생: {str(e)}")
        return []

if __name__ == "__main__":
    # 테스트 실행
    test_query = "최근 중동 정세 악화로 인한 국제 유가 상승 전망"
    print(f"🔍 '{test_query}' 시맨틱 검색 중...\n")
    
    results = fetch_economic_news(test_query, num_results=3)
    
    for idx, news in enumerate(results, 1):
        print(f"[{idx}] {news['title']}")
        print(f"🔗 링크: {news['url']}")
        if news['highlights']:
            print(f"📝 요약(단서): {' '.join(news['highlights'])}")
        print("-" * 50)
