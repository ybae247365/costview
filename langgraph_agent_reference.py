import sys
try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except AttributeError:
    pass

import os
from dotenv import load_dotenv

load_dotenv()

# API 키 설정
if not os.getenv("OPENAI_API_KEY"):
    os.environ["OPENAI_API_KEY"] = "your-openai-api-key"

# EXA API 키 (https://exa.ai 에서 발급)
if not os.getenv("EXA_API_KEY"):
    os.environ["EXA_API_KEY"] = "your-exa-api-key"

print("✅ 환경 설정 완료!")


###

from langchain_core.tools import tool
from exa_py import Exa

# EXA 클라이언트 초기화
exa_client = Exa(api_key=os.environ["EXA_API_KEY"])

@tool
def exa_web_search(query: str) -> str:
    """
    EXA를 사용하여 웹에서 최신 정보를 검색합니다.
    최신 뉴스, 기술 트렌드, 연구 논문 등을 검색할 때 사용하세요.
    
    Args:
        query: 검색할 질문이나 키워드
    """ 
    results = exa_client.search_and_contents(
        query=query,
        num_results=5,
        text={"max_characters": 1000},
        highlights={"num_sentences": 3}
    )
    
    output = []
    for result in results.results:
        output.append(f"**Title**: {result.title}")
        output.append(f"**URL**: {result.url}")
        if result.highlights:
            output.append(f"**Highlights**: {' '.join(result.highlights)}")
        output.append("---")
    
    return "\n".join(output)

print("✅ EXA Search Tool 생성 완료!")


###

# EXA 검색 테스트
# test_result = exa_web_search.invoke({"query": "LangGraph agent framework"})
# print("🔍 EXA 검색 테스트:\n")
# print(test_result[:1000] + "..." if len(test_result) > 1000 else test_result)


###
from langchain.agents import create_agent
from langchain_openai import ChatOpenAI

# LLM 설정
llm = ChatOpenAI(model="gpt-5-mini", temperature=0)

# 도구 리스트
tools = [exa_web_search]

# 에이전트 생성 (Legacy 방법 참고용)
# langchain_agent = create_agent(llm, tools)
# print("✅ LangChain ReAct 에이전트 생성 완료!")


###

from typing import Annotated, Literal
from langgraph.graph import StateGraph, MessagesState, START, END
from langgraph.prebuilt import ToolNode
from langchain_core.messages import HumanMessage, AIMessage

# MessagesState: 메시지 리스트를 자동으로 관리하는 내장 상태
# messages 필드가 자동으로 누적됨

print("📊 MessagesState 구조:")
print("  - messages: Annotated[list, add_messages]")
print("  - 메시지가 자동으로 리스트에 추가됨")


###

from datetime import datetime
today = datetime.now().strftime("%Y년 %m월 %d일")

# LLM에 도구 바인딩
llm_with_tools = llm.bind_tools(tools)

# 1. 모델 호출 노드
def call_model(state: MessagesState):
    """LLM을 호출하여 응답 또는 도구 호출 결정"""
    print("---CALL MODEL---")
    
    # 시스템 프롬프트
    system_message = {
        "role": "system",
        "content": f"""You are a helpful AI assistant with web search capabilities. Today is {today}. 
Use exa_web_search for questions requiring current information with citations. Please always respond in Korean."""
    }
    
    messages = [system_message] + state["messages"]
    response = llm_with_tools.invoke(messages)
    
    return {"messages": [response]}

# 2. 조건부 라우팅 함수
def should_continue(state: MessagesState) -> Literal["tools", "__end__"]:
    """도구 호출 필요 여부 판단"""
    last_message = state["messages"][-1]
    
    # tool_calls가 있으면 도구 실행
    if hasattr(last_message, 'tool_calls') and last_message.tool_calls:
        print("---ROUTING TO TOOLS---")
        return "tools"
    
    # 없으면 종료
    print("---ROUTING TO END---")
    return "__end__"

# 3. ToolNode 생성 (도구 실행 담당)
tool_node = ToolNode(tools)

print("✅ 노드 및 라우팅 함수 정의 완료!")


###

# 그래프 조립
workflow = StateGraph(MessagesState)

# 노드 추가
workflow.add_node("call_model", call_model)
workflow.add_node("tools", tool_node)

# 엣지 연결
workflow.add_edge(START, "call_model")

# 조건부 엣지: call_model 후 도구 호출 여부에 따라 분기
workflow.add_conditional_edges(
    "call_model",
    should_continue,
    {
        "tools": "tools",
        "__end__": END
    }
)

# 도구 실행 후 다시 모델 호출
workflow.add_edge("tools", "call_model")

# 그래프 컴파일
langgraph_agent = workflow.compile()

print("✅ LangGraph Tool Call Routing 에이전트 컴파일 완료!")
