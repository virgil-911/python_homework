from typing import Optional, Union
from decimal import Decimal
import hashlib
import json
import time
from uuid import uuid4

from ..repositories.computation_repository import ComputationRepository
from ..cache import cache_client
from ..messaging import publish_event
from ..models.domain import ComputationRequest, ComputationResult
from opentelemetry import trace

tracer = trace.get_tracer(__name__)

class MathService:
    def __init__(self, repo: ComputationRepository):
        self.repo = repo
    
    async def compute_power(self, base: float, exponent: int, cache_ttl: int = 3600) -> ComputationResult:
        with tracer.start_as_current_span("compute_power") as span:
            span.set_attribute("math.operation", "power")
            span.set_attribute("math.base", base)
            span.set_attribute("math.exponent", exponent)
            
            # Generate cache key
            cache_key = self._generate_cache_key("power", base=base, exponent=exponent)
            
            # Check cache
            cached_result = await cache_client.get(cache_key)
            if cached_result:
                result = json.loads(cached_result)
                result["cached"] = True
                await self._log_computation("power", {"base": base, "exponent": exponent}, result["result"], True)
                return ComputationResult(**result)
            
            # Compute
            start_time = time.time()
            result = self._fast_power(base, exponent)
            computation_time = (time.time() - start_time) * 1000
            
            # Create result
            computation_result = ComputationResult(
                request_id=str(uuid4()),
                result=str(result),
                computation_time_ms=computation_time,
                cached=False
            )
            
            # Cache result
            await cache_client.setex(cache_key, cache_ttl, computation_result.json())
            
            # Log to database and message broker
            await self._log_computation("power", {"base": base, "exponent": exponent}, str(result), False)
            
            return computation_result
    
    async def compute_fibonacci(self, n: int, cache_ttl: int = 3600) -> ComputationResult:
        with tracer.start_as_current_span("compute_fibonacci") as span:
            span.set_attribute("math.operation", "fibonacci")
            span.set_attribute("math.n", n)
            
            cache_key = self._generate_cache_key("fibonacci", n=n)
            
            cached_result = await cache_client.get(cache_key)
            if cached_result:
                result = json.loads(cached_result)
                result["cached"] = True
                await self._log_computation("fibonacci", {"n": n}, result["result"], True)
                return ComputationResult(**result)
            
            start_time = time.time()
            result = self._matrix_fibonacci(n) if n > 1000 else self._iterative_fibonacci(n)
            computation_time = (time.time() - start_time) * 1000
            
            computation_result = ComputationResult(
                request_id=str(uuid4()),
                result=str(result),
                computation_time_ms=computation_time,
                cached=False
            )
            
            await cache_client.setex(cache_key, cache_ttl, computation_result.json())
            await self._log_computation("fibonacci", {"n": n}, str(result), False)
            
            return computation_result
    
    async def compute_factorial(self, n: int, cache_ttl: int = 3600) -> ComputationResult:
        with tracer.start_as_current_span("compute_factorial") as span:
            span.set_attribute("math.operation", "factorial")
            span.set_attribute("math.n", n)
            
            if n > 50000:
                raise ValueError("Factorial input too large (max: 50000)")
            
            cache_key = self._generate_cache_key("factorial", n=n)
            
            cached_result = await cache_client.get(cache_key)
            if cached_result:
                result = json.loads(cached_result)
                result["cached"] = True
                await self._log_computation("factorial", {"n": n}, result["result"], True)
                return ComputationResult(**result)
            
            start_time = time.time()
            result = self._optimized_factorial(n)
            computation_time = (time.time() - start_time) * 1000
            
            computation_result = ComputationResult(
                request_id=str(uuid4()),
                result=str(result),
                computation_time_ms=computation_time,
                cached=False
            )
            
            await cache_client.setex(cache_key, cache_ttl, computation_result.json())
            await self._log_computation("factorial", {"n": n}, str(result), False)
            
            return computation_result
    
    def _fast_power(self, base: float, exp: int) -> Union[int, float]:
        """Fast exponentiation using binary method"""
        if exp == 0:
            return 1
        if exp < 0:
            return 1 / self._fast_power(base, -exp)
        
        result = 1
        base_power = base
        while exp > 0:
            if exp % 2 == 1:
                result *= base_power
            base_power *= base_power
            exp //= 2
        return result
    
    def _iterative_fibonacci(self, n: int) -> int:
        """Iterative Fibonacci for smaller values"""
        if n <= 1:
            return n
        a, b = 0, 1
        for _ in range(2, n + 1):
            a, b = b, a + b
        return b
    
    def _matrix_fibonacci(self, n: int) -> int:
        """Matrix multiplication method for large Fibonacci numbers"""
        def matrix_mult(a, b):
            return [
                [a[0][0]*b[0][0] + a[0][1]*b[1][0], a[0][0]*b[0][1] + a[0][1]*b[1][1]],
                [a[1][0]*b[0][0] + a[1][1]*b[1][0], a[1][0]*b[0][1] + a[1][1]*b[1][1]]
            ]
        
        def matrix_power(matrix, n):
            if n == 1:
                return matrix
            if n % 2 == 0:
                half = matrix_power(matrix, n // 2)
                return matrix_mult(half, half)
            return matrix_mult(matrix, matrix_power(matrix, n - 1))
        
        if n <= 1:
            return n
        base_matrix = [[1, 1], [1, 0]]
        result_matrix = matrix_power(base_matrix, n)
        return result_matrix[0][1]
    
    def _optimized_factorial(self, n: int) -> int:
        """Optimized factorial with divide-and-conquer for large numbers"""
        if n <= 1:
            return 1
        if n <= 100:
            result = 1
            for i in range(2, n + 1):
                result *= i
            return result
        
        # Divide and conquer for larger factorials
        mid = n // 2
        return self._optimized_factorial(mid) * self._product_range(mid + 1, n)
    
    def _product_range(self, start: int, end: int) -> int:
        """Helper for factorial divide-and-conquer"""
        if start > end:
            return 1
        if start == end:
            return start
        mid = (start + end) // 2
        return self._product_range(start, mid) * self._product_range(mid + 1, end)
    
    def _generate_cache_key(self, operation: str, **params) -> str:
        """Generate deterministic cache key"""
        key_data = f"{operation}:{json.dumps(params, sort_keys=True)}"
        return f"math:{hashlib.sha256(key_data.encode()).hexdigest()}"
    
    async def _log_computation(self, operation: str, params: dict, result: str, cached: bool):
        """Log computation to database and message broker"""
        await self.repo.save_computation(operation, params, result, cached)
        await publish_event("computation.completed", {
            "operation": operation,
            "params": params,
            "cached": cached,
            "timestamp": time.time()
        })