# Trading-Data_Pipeline Repository
데이터 수집, 가공, 적재 + 실시간 판단 후보 도출까지 담당

## 개요
**데이터 소스**: Binance Futures API  
**데이터 주기**: 1분, 15분, 1시간 봉  
**저장소**: AWS S3, ClickHouse (실시간 판단용), Snowflake (정밀 추론용)  
**실행 주기**: Airflow DAG 기준, 1분 / 15분 / 1시간 스케줄링

## 시스템 흐름
1. Binance API로 OHLCV 수집 → Parquet 변환 → S3 저장
2. 같은 데이터로 ClickHouse에 Insert → 실시간 전략 판단 후보 검출
3. 매수/매도 후보 발생 시 → Kafka 이벤트 발행
4. 별도 inference 서버가 Snowflake에 쿼리 + AI 추론 수행
5. 결과 따라 거래소 API로 주문 실행

## 주요 기능
| 기능               | 설명                                              |
| ---------------- | ----------------------------------------------- |
| 바이낸스 데이터 수집    | 심볼, 봉 간격에 따른 OHLCV 수집                                |
| Parquet 변환       | pandas 기반 데이터 포맷 변환                                 |
| S3 업로드           | 경로 기반 버킷 구조로 저장 (날짜/심볼 단위)                        |
| ClickHouse 적재     | 실시간 판단을 위한 Insert (1~3일치만 유지)                        |
| 후보 판단 (core 연동) | 지표 기반 매수/매도 후보 판단. Kafka로 이벤트 발행                    |
| Snowflake 추론     | 후보 발생 시 Snowflake에서 최신 S3 데이터 기반 AI 판단 수행         |
| Airflow 스케줄링     | DAG을 통해 주기적 실행 관리                                   |

## 사용 기술 스택
| 구분           | 도구 / 라이브러리                                 |
| ------------ | ------------------------------------------ |
| 스케줄링         | Apache Airflow                               |
| 데이터 처리       | pandas, pyarrow                              |
| 클라우드 스토리지    | AWS S3 (boto3)                               |
| 실시간 DB        | ClickHouse                                   |
| 정밀 추론/AI      | Snowflake + Snowpark ML / Cortex              |
| 이벤트 큐        | Kafka                                        |
| 전략 판단 / 실행   | FastAPI, gRPC, Redis (선택) 등                          |

## TODO

### ✅ 최우선

- [X] Binance API로 1m, 15m, 1h OHLCV 수집
- [X] 심볼/간격/시간 파라미터 처리
- [X] DataFrame 정제 (정렬, 중복 제거, 컬럼 통일)
- [X] Parquet 저장
- [X] S3 업로드 (`ohlcv/{interval}/{symbol}/{yyyy-MM-dd_HH}.parquet`)
- [ ] ClickHouse Insert 쿼리 작성
- [X] Airflow DAG 구성 (fetch → format → upload → insert)
- [X] DAG 주기 설정 (1m/15m/1h)

---

### 🟡 중간 우선

- [ ] ClickHouse 후보 판단 로직 (지표 기반)
- [ ] Kafka 메시지 발행
- [ ] 지표 판단 모듈화 (RSI, EMA, MACD 등)
- [X] Snowflake external stage / snowpipe ingest
- [ ] Snowflake에서 후보 판단 시 S3 기반 inference 구조 확정
- [ ] AI 판단 후 매수/매도 결정 → 주문 시스템 연동
- [X] `.env` / `config.yaml` 설정 분리

---

### 🔵 후순위

- [ ] S3 기반 전략 백테스트 (core/backtest 연동)
- [ ] Snowflake 기반 분석/리포트 자동화
- [ ] ClickHouse에 RSI, EMA 등 선계산 구조 (materialized view)
- [ ] 후보 판단 속도 개선용 캐싱 적용
- [ ] missing candle 감지
- [ ] 거래량 0인 봉 필터링 (선택 적용)
