# Trading-Data\_Pipeline Repository

데이터 수집, 가공, 적재 + 실시간 판단 후보 도출까지 담당

## 개요

**데이터 소스**: Binance Futures API
**데이터 주기**: 1분, 15분, 1시간 봉
**저장소**: AWS S3, Redis (실시간 판단용), Snowflake (정밀 추론용)
**실행 주기**: Airflow DAG 기준, 1분 / 15분 / 1시간 스케줄링

## 시스템 흐름

1. Binance API로 OHLCV 수집 → Parquet 변환 → S3 저장
2. 1분봉은 Redis에 저장 (슬롯 기반: 15개) → 실시간 판단용
3. 15분/1시간 데이터는 Snowflake에 COPY INTO로 적재
4. Redis에 저장된 1분봉 기반으로 실시간 지표 계산 및 전략 판단
5. Snowflake 데이터는 후속 AI 판단에 사용 가능

## 주요 기능

| 기능           | 설명                                                       |
| ------------ | -------------------------------------------------------- |
| 바이낸스 데이터 수집  | 심볼, 봉 간격에 따른 OHLCV 수집                                    |
| Parquet 변환   | pandas 기반 데이터 포맷 변환                                      |
| S3 업로드       | 경로 기반 버킷 구조로 저장 (interval/symbol/yyyyMMdd\_HHmm.parquet) |
| Redis 저장     | 1분봉 15개 유지 (slot 인덱스 기반 덮어쓰기)                            |
| Snowflake 적재 | 15m, 1h 주기로 S3 → COPY INTO 수행                            |
| Airflow 스케줄링 | DAG을 통해 주기적 실행 관리                                        |

## 사용 기술 스택

| 구분         | 도구 / 라이브러리              |
| ---------- | ----------------------- |
| 스케줄링       | Apache Airflow          |
| 데이터 처리     | pandas, pyarrow         |
| 클라우드 스토리지  | AWS S3 (boto3)          |
| 실시간 저장소    | Redis                   |
| 정밀 추론/AI   | Snowflake + Snowpark ML |
| 전략 판단 / 실행 | FastAPI, gRPC 등         |

## TODO

### ✅ 최우선

* [x] Binance API로 1m, 15m, 1h OHLCV 수집
* [x] 심볼/간격/시간 파라미터 처리
* [x] DataFrame 정제 (정렬, 중복 제거, 컬럼 통일)
* [x] Parquet 저장
* [x] S3 업로드 (`ohlcv/{interval}/{symbol}/{yyyyMMdd_HHmm}.parquet`)
* [x] Redis에 최근 15개 1분봉 저장 (slot 기반)
* [x] Snowflake COPY INTO 정상 작동 확인
* [x] Airflow DAG 구성 (fetch → format → upload → insert)
* [x] DAG 주기 설정 (1m/15m/1h)

---

### 🟡 중간 우선

* [ ] Redis 기반 지표 판단 로직 (RSI, EMA, MACD 등)
* [ ] 판단 모듈화 및 전략별 분리
* [ ] Kafka 메시지 발행 연동 (후보 발생 시)
* [ ] Snowflake 기반 inference 구조 설계 확정
* [ ] AI 판단 후 매수/매도 결정 → 주문 시스템 연동
* [ ] `.env` / `config.yaml` 설정 분리 완료

---

### 🔵 후순위

* [ ] S3 기반 전략 백테스트 (core/backtest 연동)
* [ ] Snowflake 기반 분석/리포트 자동화
* [ ] Redis → ClickHouse 마이그레이션 여부 검토
* [ ] Redis TTL 기반 자동 만료 도입 고려
* [ ] missing candle 감지
* [ ] 거래량 0인 봉 필터링 (선택 적용)
