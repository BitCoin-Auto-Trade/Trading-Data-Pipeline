"""
최적화된 기술적 지표 계산기
- 증분 계산, 메모리 효율성, 상태 캐싱, 성능 모니터링
"""
import pandas as pd
import numpy as np
import time
from typing import Dict, Optional, List, Any
from dataclasses import dataclass
from src.utils.logger import get_logger

@dataclass
class IndicatorState:
    """지표 계산 상태 저장"""
    ema_state: Dict[int, float] = None
    rsi_gain: Optional[float] = None
    rsi_loss: Optional[float] = None
    macd_ema12: Optional[float] = None
    macd_ema26: Optional[float] = None
    macd_signal: Optional[float] = None
    atr_tr_values: Optional[List[float]] = None
    
    def __post_init__(self):
        if self.ema_state is None:
            self.ema_state = {}
        if self.atr_tr_values is None:
            self.atr_tr_values = []

class IndicatorCalculator:
    """기술적 지표 계산기"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        
        # 상태 캐싱 (심볼별로 관리)
        self.states: Dict[str, IndicatorState] = {}
        
        # 성능 모니터링
        self.calculation_times = {}
        self.total_calculations = 0
        self.cache_hits = 0
        
        # 설정
        self.max_cache_size = 100  # 최대 캐시 크기
        self.enable_vectorization = True  # 벡터화 연산 사용
    
    def get_state(self, symbol: str) -> IndicatorState:
        """심볼별 상태 가져오기"""
        if symbol not in self.states:
            self.states[symbol] = IndicatorState()
        return self.states[symbol]
    
    def clear_state(self, symbol: str = None):
        """상태 초기화"""
        if symbol:
            if symbol in self.states:
                del self.states[symbol]
                self.logger.debug(f"{symbol} 상태 초기화됨")
        else:
            self.states.clear()
            self.logger.info("모든 상태 초기화됨")
    
    def format_klines(self, raw_klines: list) -> Optional[pd.DataFrame]:
        """원시 kline 목록을 형식화된 DataFrame으로 변환 (메모리 최적화)"""
        if not raw_klines:
            self.logger.warning("비어 있는 원시 kline 데이터를 받았습니다.")
            return None
        
        start_time = time.time()
        
        try:
            # 메모리 효율적인 DataFrame 생성
            df = pd.DataFrame(raw_klines, columns=[
                'open_time', 'open', 'high', 'low', 'close', 'volume', 
                'close_time', 'quote_asset_volume', 'number_of_trades',
                'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
            ])
            
            # 메모리 절약을 위해 데이터 타입 변환
            df = df.astype({
                'open': 'float32',
                'high': 'float32',
                'low': 'float32', 
                'close': 'float32',
                'volume': 'float32'
            })
            
            # 시간 인덱스 설정 (효율적)
            df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
            df.set_index('open_time', inplace=True)
            
            # 불필요한 컬럼 즉시 제거 (메모리 절약)
            df = df[['open', 'high', 'low', 'close', 'volume']]
            
            # NaN 값 처리
            if df.isna().any().any():
                self.logger.warning("NaN 값이 발견되어 제거합니다.")
                df = df.dropna()
            
            elapsed_time = time.time() - start_time
            self.calculation_times['format'] = elapsed_time
            
            self.logger.info(f"{len(df)}개의 kline을 DataFrame으로 형식화했습니다. ({elapsed_time:.3f}초)")
            return df
            
        except Exception as e:
            self.logger.error(f"DataFrame 형식화 중 오류: {e}")
            return None
    
    def calculate_ema_incremental(self, df: pd.DataFrame, period: int, symbol: str = "default") -> pd.DataFrame:
        """증분 EMA 계산 (이전 상태 활용)"""
        start_time = time.time()
        col_name = f'ema_{period}'
        
        try:
            state = self.get_state(symbol)
            alpha = 2.0 / (period + 1)
            
            if period in state.ema_state and len(df) > 0:
                # 증분 계산 (훨씬 빠름)
                prev_ema = state.ema_state[period]
                ema_values = []
                
                for price in df['close']:
                    prev_ema = alpha * price + (1 - alpha) * prev_ema
                    ema_values.append(prev_ema)
                
                df[col_name] = ema_values
                state.ema_state[period] = prev_ema
                self.cache_hits += 1
                
            else:
                # 초기 계산
                df[col_name] = df['close'].ewm(span=period, adjust=False).mean()
                if len(df) > 0:
                    state.ema_state[period] = df[col_name].iloc[-1]
            
            elapsed_time = time.time() - start_time
            self.calculation_times[f'ema_{period}'] = elapsed_time
            self.logger.debug(f"EMA({period}) 계산 완료: {elapsed_time:.3f}초")
            
            return df
            
        except Exception as e:
            self.logger.error(f"EMA 계산 중 오류: {e}")
            return df
    
    def calculate_rsi_incremental(self, df: pd.DataFrame, period: int = 14, symbol: str = "default") -> pd.DataFrame:
        """증분 RSI 계산"""
        start_time = time.time()
        col_name = f'rsi_{period}'
        
        try:
            state = self.get_state(symbol)
            
            if len(df) < 2:
                df[col_name] = np.nan
                return df
            
            # 가격 변화 계산
            delta = df['close'].diff()
            
            if state.rsi_gain is not None and state.rsi_loss is not None:
                # 증분 계산
                alpha = 1.0 / period
                gains = delta.where(delta > 0, 0)
                losses = -delta.where(delta < 0, 0)
                
                avg_gains = []
                avg_losses = []
                current_gain = state.rsi_gain
                current_loss = state.rsi_loss
                
                for gain, loss in zip(gains, losses):
                    current_gain = alpha * gain + (1 - alpha) * current_gain
                    current_loss = alpha * loss + (1 - alpha) * current_loss
                    avg_gains.append(current_gain)
                    avg_losses.append(current_loss)
                
                state.rsi_gain = current_gain
                state.rsi_loss = current_loss
                self.cache_hits += 1
                
            else:
                # 초기 계산
                gains = delta.where(delta > 0, 0)
                losses = -delta.where(delta < 0, 0)
                avg_gains = gains.ewm(span=period, adjust=False).mean()
                avg_losses = losses.ewm(span=period, adjust=False).mean()
                
                if len(avg_gains) > 0 and len(avg_losses) > 0:
                    state.rsi_gain = avg_gains.iloc[-1]
                    state.rsi_loss = avg_losses.iloc[-1]
            
            # RSI 계산
            avg_gains = pd.Series(avg_gains, index=df.index)
            avg_losses = pd.Series(avg_losses, index=df.index)
            rs = avg_gains / avg_losses.replace(0, np.inf)
            df[col_name] = 100 - (100 / (1 + rs))
            
            elapsed_time = time.time() - start_time
            self.calculation_times[f'rsi_{period}'] = elapsed_time
            self.logger.debug(f"RSI({period}) 계산 완료: {elapsed_time:.3f}초")
            
            return df
            
        except Exception as e:
            self.logger.error(f"RSI 계산 중 오류: {e}")
            return df
    
    def calculate_macd_incremental(self, df: pd.DataFrame, short_period: int = 12, 
                                 long_period: int = 26, signal_period: int = 9, 
                                 symbol: str = "default") -> pd.DataFrame:
        """증분 MACD 계산"""
        start_time = time.time()
        
        try:
            state = self.get_state(symbol)
            alpha_short = 2.0 / (short_period + 1)
            alpha_long = 2.0 / (long_period + 1)
            alpha_signal = 2.0 / (signal_period + 1)
            
            if (state.macd_ema12 is not None and state.macd_ema26 is not None 
                and state.macd_signal is not None):
                # 증분 계산
                ema12_values = []
                ema26_values = []
                macd_values = []
                signal_values = []
                
                current_ema12 = state.macd_ema12
                current_ema26 = state.macd_ema26
                current_signal = state.macd_signal
                
                for price in df['close']:
                    # EMA 계산
                    current_ema12 = alpha_short * price + (1 - alpha_short) * current_ema12
                    current_ema26 = alpha_long * price + (1 - alpha_long) * current_ema26
                    
                    # MACD 계산
                    macd_val = current_ema12 - current_ema26
                    
                    # Signal 계산
                    current_signal = alpha_signal * macd_val + (1 - alpha_signal) * current_signal
                    
                    ema12_values.append(current_ema12)
                    ema26_values.append(current_ema26)
                    macd_values.append(macd_val)
                    signal_values.append(current_signal)
                
                df['macd'] = macd_values
                df['macd_signal'] = signal_values
                df['macd_hist'] = np.array(macd_values) - np.array(signal_values)
                
                # 상태 업데이트
                state.macd_ema12 = current_ema12
                state.macd_ema26 = current_ema26
                state.macd_signal = current_signal
                self.cache_hits += 1
                
            else:
                # 초기 계산
                ema_short = df['close'].ewm(span=short_period, adjust=False).mean()
                ema_long = df['close'].ewm(span=long_period, adjust=False).mean()
                df['macd'] = ema_short - ema_long
                df['macd_signal'] = df['macd'].ewm(span=signal_period, adjust=False).mean()
                df['macd_hist'] = df['macd'] - df['macd_signal']
                
                # 상태 저장
                if len(df) > 0:
                    state.macd_ema12 = ema_short.iloc[-1]
                    state.macd_ema26 = ema_long.iloc[-1]
                    state.macd_signal = df['macd_signal'].iloc[-1]
            
            elapsed_time = time.time() - start_time
            self.calculation_times['macd'] = elapsed_time
            self.logger.debug(f"MACD 계산 완료: {elapsed_time:.3f}초")
            
            return df
            
        except Exception as e:
            self.logger.error(f"MACD 계산 중 오류: {e}")
            return df
    
    def calculate_atr_optimized(self, df: pd.DataFrame, period: int = 14, symbol: str = "default") -> pd.DataFrame:
        """최적화된 ATR 계산"""
        start_time = time.time()
        col_name = f'atr_{period}'
        
        try:
            if len(df) < 2:
                df[col_name] = np.nan
                return df
            
            # True Range 계산 (벡터화)
            high_low = df['high'] - df['low']
            high_close = np.abs(df['high'] - df['close'].shift(1))
            low_close = np.abs(df['low'] - df['close'].shift(1))
            
            tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
            
            # ATR 계산
            df[col_name] = tr.ewm(span=period, adjust=False).mean()
            
            elapsed_time = time.time() - start_time
            self.calculation_times[f'atr_{period}'] = elapsed_time
            self.logger.debug(f"ATR({period}) 계산 완료: {elapsed_time:.3f}초")
            
            return df
            
        except Exception as e:
            self.logger.error(f"ATR 계산 중 오류: {e}")
            return df
    
    def calculate_adx_optimized(self, df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """최적화된 ADX 계산 (벡터화)"""
        start_time = time.time()
        
        try:
            if len(df) < period + 1:
                df['adx'] = np.nan
                return df
            
            # 벡터화된 계산
            df['up_move'] = df['high'].diff()
            df['down_move'] = -df['low'].diff()
            
            # Plus/Minus DM 계산
            df['plus_dm'] = np.where(
                (df['up_move'] > df['down_move']) & (df['up_move'] > 0), 
                df['up_move'], 0
            )
            df['minus_dm'] = np.where(
                (df['down_move'] > df['up_move']) & (df['down_move'] > 0), 
                df['down_move'], 0
            )
            
            # True Range
            high_low = df['high'] - df['low']
            high_close = np.abs(df['high'] - df['close'].shift(1))
            low_close = np.abs(df['low'] - df['close'].shift(1))
            tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
            
            # 평활화
            atr = tr.ewm(span=period, adjust=False).mean()
            plus_di = 100 * (df['plus_dm'].ewm(span=period, adjust=False).mean() / atr)
            minus_di = 100 * (df['minus_dm'].ewm(span=period, adjust=False).mean() / atr)
            
            # DX 및 ADX 계산
            dx = 100 * np.abs(plus_di - minus_di) / (plus_di + minus_di)
            df['adx'] = dx.ewm(span=period, adjust=False).mean()
            
            # 임시 컬럼 제거
            df.drop(['up_move', 'down_move', 'plus_dm', 'minus_dm'], axis=1, inplace=True)
            
            elapsed_time = time.time() - start_time
            self.calculation_times['adx'] = elapsed_time
            self.logger.debug(f"ADX({period}) 계산 완료: {elapsed_time:.3f}초")
            
            return df
            
        except Exception as e:
            self.logger.error(f"ADX 계산 중 오류: {e}")
            return df
    
    def calculate_sma_optimized(self, df: pd.DataFrame, period: int) -> pd.DataFrame:
        """최적화된 SMA 계산"""
        start_time = time.time()
        col_name = f'sma_{period}'
        
        try:
            # 벡터화된 롤링 평균
            df[col_name] = df['close'].rolling(window=period, min_periods=1).mean()
            
            elapsed_time = time.time() - start_time
            self.calculation_times[f'sma_{period}'] = elapsed_time
            self.logger.debug(f"SMA({period}) 계산 완료: {elapsed_time:.3f}초")
            
            return df
            
        except Exception as e:
            self.logger.error(f"SMA 계산 중 오류: {e}")
            return df
    
    def calculate_bollinger_bands_optimized(self, df: pd.DataFrame, period: int = 20, std_dev: int = 2) -> pd.DataFrame:
        """최적화된 볼린저 밴드 계산"""
        start_time = time.time()
        
        try:
            # 벡터화된 계산
            sma = df['close'].rolling(window=period).mean()
            std = df['close'].rolling(window=period).std()
            
            df['bb_upper'] = sma + (std * std_dev)
            df['bb_middle'] = sma
            df['bb_lower'] = sma - (std * std_dev)
            
            elapsed_time = time.time() - start_time
            self.calculation_times['bollinger'] = elapsed_time
            self.logger.debug(f"Bollinger Bands({period}, {std_dev}) 계산 완료: {elapsed_time:.3f}초")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Bollinger Bands 계산 중 오류: {e}")
            return df
    
    def calculate_stochastic_optimized(self, df: pd.DataFrame, k_period: int = 14, d_period: int = 3) -> pd.DataFrame:
        """최적화된 스토캐스틱 계산"""
        start_time = time.time()
        
        try:
            # 벡터화된 계산
            low_min = df['low'].rolling(window=k_period).min()
            high_max = df['high'].rolling(window=k_period).max()
            
            df['stoch_k'] = 100 * ((df['close'] - low_min) / (high_max - low_min))
            df['stoch_d'] = df['stoch_k'].rolling(window=d_period).mean()
            
            elapsed_time = time.time() - start_time
            self.calculation_times['stochastic'] = elapsed_time
            self.logger.debug(f"Stochastic({k_period}, {d_period}) 계산 완료: {elapsed_time:.3f}초")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Stochastic 계산 중 오류: {e}")
            return df
    
    def calculate_volume_sma(self, df: pd.DataFrame, period: int = 20) -> pd.DataFrame:
        """거래량 이동평균(SMA) 계산"""
        start_time = time.time()
        col_name = f'volume_sma_{period}'
        try:
            df[col_name] = df['volume'].rolling(window=period, min_periods=1).mean()
            elapsed_time = time.time() - start_time
            self.calculation_times[col_name] = elapsed_time
            self.logger.debug(f"Volume SMA({period}) 계산 완료: {elapsed_time:.3f}초")
            return df
        except Exception as e:
            self.logger.error(f"Volume SMA 계산 중 오류: {e}")
            return df

    def calculate_volume_ratio(self, df: pd.DataFrame, period: int = 20) -> pd.DataFrame:
        """거래량 비율 계산"""
        start_time = time.time()
        col_name = 'volume_ratio'
        sma_col_name = f'volume_sma_{period}'
        try:
            if sma_col_name not in df.columns:
                df = self.calculate_volume_sma(df, period)
            df[col_name] = df['volume'] / df[sma_col_name]
            elapsed_time = time.time() - start_time
            self.calculation_times[col_name] = elapsed_time
            self.logger.debug(f"Volume Ratio 계산 완료: {elapsed_time:.3f}초")
            return df
        except Exception as e:
            self.logger.error(f"Volume Ratio 계산 중 오류: {e}")
            return df

    def calculate_price_momentum(self, df: pd.DataFrame, period: int = 5) -> pd.DataFrame:
        """가격 모멘텀 계산"""
        start_time = time.time()
        col_name = f'price_momentum_5m'
        try:
            df[col_name] = df['close'].diff(period)
            elapsed_time = time.time() - start_time
            self.calculation_times[col_name] = elapsed_time
            self.logger.debug(f"Price Momentum({period}) 계산 완료: {elapsed_time:.3f}초")
            return df
        except Exception as e:
            self.logger.error(f"Price Momentum 계산 중 오류: {e}")
            return df

    def calculate_volatility(self, df: pd.DataFrame, period: int = 20) -> pd.DataFrame:
        """변동성 계산 (로그 수익률의 표준편차)"""
        start_time = time.time()
        col_name = f'volatility_20d'
        try:
            log_returns = np.log(df['close'] / df['close'].shift(1))
            df[col_name] = log_returns.rolling(window=period).std() * np.sqrt(period)
            elapsed_time = time.time() - start_time
            self.calculation_times[col_name] = elapsed_time
            self.logger.debug(f"Volatility({period}) 계산 완료: {elapsed_time:.3f}초")
            return df
        except Exception as e:
            self.logger.error(f"Volatility 계산 중 오류: {e}")
            return df
    
    def calculate_all_indicators_batch(self, df: pd.DataFrame, symbol: str = "default") -> pd.DataFrame:
        """모든 지표를 배치로 효율적 계산"""
        if df.empty:
            self.logger.warning("빈 DataFrame입니다. 지표 계산을 건너뜁니다.")
            return df
        
        total_start_time = time.time()
        self.total_calculations += 1
        
        try:
            self.logger.debug(f"총 {len(df)}개 행에 대해 지표 계산 시작...")
            
            # 순서대로 계산 (의존성 고려)
            df = self.calculate_ema_incremental(df, 20, symbol)
            df = self.calculate_rsi_incremental(df, 14, symbol)
            df = self.calculate_macd_incremental(df, symbol=symbol)
            df = self.calculate_atr_optimized(df, 14, symbol)
            df = self.calculate_adx_optimized(df, 14)
            df = self.calculate_sma_optimized(df, 50)
            df = self.calculate_sma_optimized(df, 200)
            df = self.calculate_bollinger_bands_optimized(df, 20)
            df = self.calculate_stochastic_optimized(df, 14, 3)
            df = self.calculate_volume_sma(df, 20)
            df = self.calculate_volume_ratio(df, 20)
            df = self.calculate_price_momentum(df, 5)
            df = self.calculate_volatility(df, 20)
            
            total_elapsed = time.time() - total_start_time
            self.calculation_times['total'] = total_elapsed
            
            self.logger.info(f"모든 지표 계산 완료: {total_elapsed:.3f}초 (캐시 히트: {self.cache_hits})")
            
            return df
            
        except Exception as e:
            self.logger.error(f"배치 지표 계산 중 오류: {e}")
            return df
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """성능 통계 조회"""
        total_time = sum(self.calculation_times.values())
        
        return {
            "total_calculations": self.total_calculations,
            "cache_hits": self.cache_hits,
            "cache_hit_rate": round(self.cache_hits / max(self.total_calculations, 1) * 100, 2),
            "calculation_times": self.calculation_times.copy(),
            "total_time": round(total_time, 3),
            "average_time_per_calculation": round(total_time / max(self.total_calculations, 1), 3),
            "active_symbols": len(self.states)
        }
    
    def reset_performance_stats(self):
        """성능 통계 초기화"""
        self.calculation_times.clear()
        self.total_calculations = 0
        self.cache_hits = 0
        self.logger.info("성능 통계가 초기화되었습니다.")
    
    # 기존 메서드명 호환성 유지
    def calculate_ema(self, df: pd.DataFrame, period: int) -> pd.DataFrame:
        """호환성 유지용 EMA 계산"""
        return self.calculate_ema_incremental(df, period)
    
    def calculate_rsi(self, df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """호환성 유지용 RSI 계산"""
        return self.calculate_rsi_incremental(df, period)
    
    def calculate_macd(self, df: pd.DataFrame, short_period: int = 12, long_period: int = 26, signal_period: int = 9) -> pd.DataFrame:
        """호환성 유지용 MACD 계산"""
        return self.calculate_macd_incremental(df, short_period, long_period, signal_period)
    
    def calculate_atr(self, df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """호환성 유지용 ATR 계산"""
        return self.calculate_atr_optimized(df, period)
    
    def calculate_adx(self, df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """호환성 유지용 ADX 계산"""
        return self.calculate_adx_optimized(df, period)
    
    def calculate_sma(self, df: pd.DataFrame, period: int) -> pd.DataFrame:
        """호환성 유지용 SMA 계산"""
        return self.calculate_sma_optimized(df, period)
    
    def calculate_bollinger_bands(self, df: pd.DataFrame, period: int = 20, std_dev: int = 2) -> pd.DataFrame:
        """호환성 유지용 볼린저 밴드 계산"""
        return self.calculate_bollinger_bands_optimized(df, period, std_dev)
    
    def calculate_stochastic_oscillator(self, df: pd.DataFrame, k_period: int = 14, d_period: int = 3) -> pd.DataFrame:
        """호환성 유지용 스토캐스틱 계산"""
        return self.calculate_stochastic_optimized(df, k_period, d_period)

# 사용 예제
if __name__ == '__main__':
    import datetime
    
    # 테스트 데이터 생성
    dates = pd.date_range(start='2024-01-01', periods=100, freq='1min')
    test_data = pd.DataFrame({
        'open': np.random.randn(100).cumsum() + 100,
        'high': np.random.randn(100).cumsum() + 102,
        'low': np.random.randn(100).cumsum() + 98,
        'close': np.random.randn(100).cumsum() + 100,
        'volume': np.random.randint(1000, 10000, 100)
    }, index=dates)

    calculator = IndicatorCalculator()

    print("=== 성능 테스트 ===")
    
    # 첫 번째 계산 (초기 계산)
    start_time = time.time()
    result1 = calculator.calculate_all_indicators_batch(test_data.copy(), "BTCUSDT")
    first_time = time.time() - start_time
    print(f"첫 번째 계산: {first_time:.3f}초")
    
    # 두 번째 계산 (증분 계산)
    new_data = test_data.tail(10).copy()  # 마지막 10개 데이터만
    start_time = time.time()
    result2 = calculator.calculate_all_indicators_batch(new_data, "BTCUSDT")
    second_time = time.time() - start_time
    print(f"두 번째 계산 (증분): {second_time:.3f}초")
    
    # 성능 통계
    stats = calculator.get_performance_stats()
    print(f"\n=== 성능 통계 ===")
    print(f"총 계산 횟수: {stats['total_calculations']}")
    print(f"캐시 히트: {stats['cache_hits']}")
    print(f"캐시 히트율: {stats['cache_hit_rate']}%")
    print(f"평균 계산 시간: {stats['average_time_per_calculation']}초")
    
    improvement = (first_time - second_time) / first_time * 100
    print(f"증분 계산 성능 개선: {improvement:.1f}%")