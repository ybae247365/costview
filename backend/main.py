from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List

app = FastAPI(title="Cost-Vue API", description="AI 기반 인과관계 저널리즘 서비스")

# 프론트엔드 연동을 위한 CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ======== MODELS ========
class ImpactChainMetrics(BaseModel):
    raw_shock: str
    wallet_hit: str
    transmission_time: str

class ImpactChain(BaseModel):
    id: int
    tags: List[str]
    title: str
    description: str
    metrics: ImpactChainMetrics
    # 그래프 데이터 용도
    months: List[str]
    raw_price_data: List[int]
    consumer_price_data: List[int]

class DashboardSnapshot(BaseModel):
    total_chains: int
    max_raw_shock: str
    max_wallet_hit: str

class DashboardResponse(BaseModel):
    snapshot: DashboardSnapshot
    chains: dict[str, List[ImpactChain]]

# ======== API ENDPOINTS ========
@app.get("/api/dashboard", response_model=DashboardResponse)
async def get_dashboard():
    """
    프론트엔드 목업 앱(Expo)에서 사용할 Dashboard API 
    (현재는 화면을 그리기 위한 Hard-coded Mock 데이터 배포, 
    이후 Reasoning Engine이 Supabase에 넣은 데이터 연동)
    """
    return {
        "snapshot": {
            "total_chains": 4,
            "max_raw_shock": "+400%",
            "max_wallet_hit": "+80%"
        },
        "chains": {
            "Energy": [
                {
                    "id": 1,
                    "tags": ["Middle East", "Energy"],
                    "title": "아랍 산유국 금수 조치, 국제 유가 4배 폭등",
                    "description": "원유 공급이 급감하면서 정유, 운송, 식품 전반 가격이 단기간에 급등했다.",
                    "metrics": {
                        "raw_shock": "+400%",
                        "wallet_hit": "+80%",
                        "transmission_time": "1개월"
                    },
                    "months": ["M0", "M1", "M2", "M3", "M4", "M5", "M6"],
                    "raw_price_data": [0, 400, 400, 400, 400, 400, 400],
                    "consumer_price_data": [0, 0, 80, 80, 80, 80, 80]
                }
            ],
            "Food": [
                 {
                    "id": 2,
                    "tags": ["Black Sea", "Food"],
                    "title": "흑해 곡물 수출 불안, 가공식품 가격 연쇄 상승",
                    "description": "곡물 조달 차질이 빵, 면, 외식비로 번지며 생활물가 부담이 커졌다.",
                    "metrics": {
                        "raw_shock": "+65%",
                        "wallet_hit": "+19%",
                        "transmission_time": "2개월"
                    },
                    "months": ["M0", "M1", "M2", "M3", "M4", "M5", "M6"],
                    "raw_price_data": [0, 65, 65, 65, 65, 65, 65],
                    "consumer_price_data": [0, 0, 0, 19, 19, 19, 19]
                }
            ],
            "Utilities": [
                 {
                    "id": 3,
                    "tags": ["Europe", "Utilities"],
                    "title": "천연가스 불안, 전기요금과 난방비에 시차 반영",
                    "description": "에너지 조달 비용 상승이 공공요금과 겨울철 난방비로 이어졌다.",
                    "metrics": {
                        "raw_shock": "+120%",
                        "wallet_hit": "+26%",
                        "transmission_time": "3개월"
                    },
                    "months": ["M0", "M1", "M2", "M3", "M4", "M5", "M6"],
                    "raw_price_data": [0, 120, 120, 120, 120, 120, 120],
                    "consumer_price_data": [0, 0, 0, 0, 26, 26, 26]
                }
            ]
        }
    }
