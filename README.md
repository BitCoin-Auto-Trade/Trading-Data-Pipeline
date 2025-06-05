# Trading-Data_Pipline Repository
데이터 수집, 가공, 적재까지 관리

## 개요
**데이터 소스**: Binance Futures API  
**데이터 주기**: 15분, 1시간 봉  
**저장소**: AWS S3  
**적재 대상**: Snowflake  
**실행 주기**: Airflow DAG 기준, 15분 / 1시간 스케줄링

## 주요 기능
| 기능           | 설명                                    |
| ------------ | ------------------------------------- |
| 바이낸스 데이터 수집  | 심볼, 봉 간격에 따른 OHLCV 데이터 수집             |
| Parquet 변환   | pandas 기반 데이터 포맷 변환                   |
| S3 업로드       | 경로 기반 버킷 구조로 저장 (날짜/심볼 단위)            |
| Snowflake 적재 | COPY INTO 명령어로 Parquet → Warehouse 적재 |
| Airflow 스케줄링 | DAG을 통해 주기적 실행 관리                     |

## 사용 기술 스택
| 구분        | 도구 / 라이브러리                             |
| --------- | -------------------------------------- |
| 스케줄링      | Apache Airflow                         |
| 데이터 처리    | pandas, pyarrow                        |
| 클라우드 스토리지 | AWS S3 (boto3)                         |
| 데이터 웨어하우스 | Snowflake                              |

## TODO

### 최우선 

#### 1. Binance OHLCV 수집
- [X] Binance Futures REST API 호출 (interval: 15m, 1h)
- [X] 심볼/간격/시간 범위 입력 파라미터 처리
- [X] pandas DataFrame 반환

#### 2. 데이터 포맷 정제
- [X] UTC 기준 정렬
- [X] 중복 제거
- [X] 컬럼 통일: `timestamp, open, high, low, close, volume, symbol, interval`
- [X] Parquet 저장 기능

#### 3. S3 업로드
- [X] boto3 기반 업로드 함수 구현
- [X] S3 경로 설계: `ohlcv/{interval}/{symbol}/{yyyy-MM-dd_HH}.parquet`
- [X] 업로드 성공/실패 로그 처리

#### 4. Snowflake 적재
- [X] COPY INTO 쿼리 실행 함수 작성
- [X] Stage 및 File Format 정의
- [X] 파일 존재 여부 체크 및 예외 처리

#### 5. Airflow DAG 작성
- [X] `fetch → format → upload → copy` DAG 구성
- [X] 주기: 15분 / 1시간 단위
- [X] 심볼, 간격 동적 파라미터 처리

---

### 중간 우선

#### 6. 에러 핸들링 / 재시도
- [ ] Binance API 실패 시 최대 5회 재시도
- [ ] S3 업로드 실패 처리
- [ ] Snowflake COPY 실패 시 로그 남기고 skip

#### 7. 설정 분리
- [X] `.env` 또는 `config.yaml` 파일 사용
- [X] Binance / AWS / Snowflake 키 관리

#### 8. 로깅 시스템
- [ ] task 단위 로그 기록 (INFO, ERROR)
- [ ] Airflow 로그 연동

---

### 후순위

#### 9. 멀티 심볼/간격 확장
- [X] BTC, ETH, SOL 등 동시 수집
- [X] DAG 내 for-loop 처리 or 동적 DAG 생성

#### 10. 지표 선계산 저장
- [ ] RSI, EMA 등 기초 지표 포함 저장
- [ ] 백테스트 연동 고려

#### 11. 데이터 검증
- [ ] missing candle 감지
- [ ] 거래량=0인 봉 필터링 (선택)
