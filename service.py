from typing import Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, insert
from datetime import datetime

from ..models.database import ComputationLog
from ..database import get_session

class ComputationRepository:
    def __init__(self, session: AsyncSession):
        self.session = session
    
    async def save_computation(self, operation: str, params: Dict[str, Any], result: str, cached: bool):
        """Persist computation to database"""
        log_entry = ComputationLog(
            operation=operation,
            parameters=params,
            result=result[:1000],  # Truncate very large results
            cached=cached,
            created_at=datetime.utcnow()
        )
        self.session.add(log_entry)
        await self.session.commit()
    
    async def get_computation_history(self, operation: str = None, limit: int = 100):
        """Retrieve computation history"""
        query = select(ComputationLog)
        if operation:
            query = query.where(ComputationLog.operation == operation)
        query = query.order_by(ComputationLog.created_at.desc()).limit(limit)
        
        result = await self.session.execute(query)
        return result.scalars().all()