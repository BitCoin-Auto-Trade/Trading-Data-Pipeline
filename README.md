# Trading-Data_Pipeline Repository

Binance 선물 데이터 수집 → 실시간 판단용 Redis 저장 → 분석용 Snowflake 저장 처리

## 개요

**데이터 소스**: Binance Futures API  
**주기**: 1분, 15분, 1시간 봉  
**저장소**: Redis (실시간 전략 판단용), Snowflake (AI 판단 및 분석용)  
**스케줄링**: Apache Airflow DAG (1분 / 15분 / 1시간)

## Redis 처리 구조 (1분봉)

- 키: `ohlcv:{symbol}:1m`
- 값: JSON 문자열 60개 (최신순)
- 형식: `LPUSH`로 넣고 `LTRIM`으로 60개 유지
- 지표 판단은 core 레포에서 처리함

---

## Snowflake 처리 구조 (15분, 1시간봉)

- 수집한 OHLCV를 Parquet 저장
- S3에 저장된 파일 → COPY INTO로 Snowflake 적재
- Redis에 별도 저장 안함

---

## 주요 기능

| 기능           | 설명                                                       |
|----------------|------------------------------------------------------------|
| Binance 수집     | 심볼/간격별 OHLCV 수집                                      |
| Parquet 변환    | pandas → pyarrow 기반 parquet 포맷                         |
| Redis 저장       | 1분봉만 저장. 60개만 유지되도록 LPUSH + LTRIM              |
| S3 업로드        | 15분/1시간 봉만 저장. 경로: `ohlcv/{interval}/{symbol}/{yyyyMMdd_HHmm}.parquet` |
| Snowflake 적재   | COPY INTO로 적재                                           |
| Airflow DAG     | 주기적 스케줄링 및 분기 처리                                |

---

## 기술 스택

| 분류          | 사용 기술                           |
|---------------|-------------------------------------|
| 수집          | Binance API, requests               |
| 데이터 가공    | pandas, pyarrow                    |
| 실시간 저장소 | Redis                               |
| 클라우드 저장 | AWS S3 (boto3)                      |
| 정밀 분석용    | Snowflake + COPY INTO              |
| 스케줄링      | Apache Airflow (DAG, TaskGroup 등) |

---

## TODO 리스트

### ✅ 완료 항목

- [x] 1m, 15m, 1h OHLCV 수집 DAG 구성
- [x] Redis에 1분봉 60개 적재
- [x] 15m, 1h S3 저장 + Snowflake COPY INTO 적재
- [x] Airflow 스케줄링 분리 (1분, 15분, 1시간)
- [x] 최초 시작시 데이터 대량 적재 DAG 구성

---

### 🟡 진행 예정

- [ ] Core에서 15m, 1h, 1m Redis 기반 지표 계산 및 매수/매도 판단
- [ ] core 판단용 Redis 지표 포맷 확정
- [ ] Snowflake ML + FastAPI 판단 분리
- [ ] 백테스트 연동 (backtest 레포)

---

### 🔵 후순위

- [ ] Redis TTL 도입 (1m 봉 자동 만료)
- [ ] missing candle 감지 DAG
- [ ] 거래량 0봉 필터링 (선택적 적용)
- [ ] ClickHouse로 마이그레이션 여부 검토