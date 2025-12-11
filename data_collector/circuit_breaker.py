import time
import threading
from functools import wraps

class CircuitBreakerOpenException(Exception):
    pass

class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=30, expected_exception=Exception):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        
        self.failure_count = 0
        self.last_failure_time = 0
        self.state = 'CLOSED'
        self.lock = threading.RLock() 

    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            return self.call(func, *args, **kwargs)
        return wrapper

    def call(self, func, *args, **kwargs):
        with self.lock:
            if self.state == 'OPEN':
                time_since_failure = time.time() - self.last_failure_time
                if time_since_failure > self.recovery_timeout:
                    self.state = 'HALF_OPEN'
                else:
                    raise CircuitBreakerOpenException(
                        f"Circuito aperto. Riprova tra {self.recovery_timeout - time_since_failure:.2f}s"
                    )
        try:
            result = func(*args, **kwargs)
        except self.expected_exception as e:
            with self.lock:
                self._handle_failure()
            raise e
        with self.lock:
            self._handle_success()
        
        return result

    def _handle_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()

        if self.state == 'HALF_OPEN':
            self.state = 'OPEN'
        elif self.failure_count >= self.failure_threshold:
            self.state = 'OPEN'

    def _handle_success(self):
        if self.state == 'HALF_OPEN':
            self.state = 'CLOSED'
            self.failure_count = 0
        elif self.state == 'CLOSED':
            self.failure_count = 0 