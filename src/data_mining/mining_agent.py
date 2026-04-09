import os
import sys
try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except AttributeError:
    pass

from typing import Annotated, Literal, List, Dict, Any
from datetime import datetime
from dotenv import load_dotenv

from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage
from langgraph.graph import StateGraph, MessagesState, START, END
from langgraph.prebuilt import ToolNode
from exa_py import Exa

load_dotenv()

# EXA API 초기화
exa_client = Exa(api_key=os.environ["EXA_API_KEY"])

@tool
def gather_cross_verified_news(query: str) -> str:
    """
    경제적 질문에 대해 여러 소스를 교차 검색하고 
    노이즈가 필터링된 핵심 하이라이트 문장들을 반환합니다 (REQ-02, REQ-04).
    """
    try:
        results = exa_client.search_and_contents(
            query=query,
            num_results=5,
            use_autoprompt=True,
            text={"max_characters": 1500},
            highlights={"num_sentences": 4}
        )
        
        output = []
        for result in results.results:
            output.append(f"[Title]: {result.title}")
            output.append(f"[Source]: {result.url}")
            if result.highlights:
                output.append(f"[Facts]: {' '.join(result.highlights)}")
            output.append("---")
            
        return "\n".join(output)
    except Exception as e:
        return f"검색 중 오류 발생: {str(e)}"

# LLM 모델 (gpt-4o-mini 등으로 대체 가능)
# 사용자의 이전 템플릿에 맞추어 모델을 설정합니다.
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
tools = [gather_cross_verified_news]
llm_with_tools = llm.bind_tools(tools)

today = datetime.now().strftime("%Y년 %m월 %d일")

def call_model(state: MessagesState):
    """LLM을 호출하여 노이즈를 필터링하고 신뢰성 높은 요약을 생성"""
    system_prompt = {
        "role": "system",
        "content": f"""You are 'Cost-Vue Fact Mint', highly analytical AI. Today is {today}.
Your task is to gather raw economic news using 'gather_cross_verified_news' tool,
cross-verify across multiple sources, remove any subjective noise or ads,
and present ONLY the clean, verified facts. Respond logically in Korean."""
    }
    
    messages = [system_prompt] + state["messages"]
    response = llm_with_tools.invoke(messages)
    
    return {"messages": [response]}

def should_continue(state: MessagesState) -> Literal["tools", "__end__"]:
    last_message = state["messages"][-1]
    
    if hasattr(last_message, 'tool_calls') and last_message.tool_calls:
        return "tools"
    return "__end__"

# 랭그래프 조립 (REQ-01, REQ-02 수집 및 1차 정제 워크플로우)
tool_node = ToolNode(tools)

workflow = StateGraph(MessagesState)
workflow.add_node("call_model", call_model)
workflow.add_node("tools", tool_node)

workflow.add_edge(START, "call_model")
workflow.add_conditional_edges("call_model", should_continue, {"tools": "tools", "__end__": END})
workflow.add_edge("tools", "call_model")

mining_agent = workflow.compile()

if __name__ == "__main__":
    test_query = "최근 브라질 가뭄으로 인한 커피 원두 가격 상승 요인과 시장 상황"
    print(f"[TEST] 마이닝 및 노이즈 필터링 에이전트 구동: '{test_query}'\n")
    
    try:
        result = mining_agent.invoke({"messages": [{"role": "user", "content": test_query}]})
        print(f"[RESULT] 최종 정제 결과 (Clean Text):\n{result['messages'][-1].content}")
    except Exception as e:
        import traceback
        print("AGENT ERROR:")
        traceback.print_exc()
