import uuid

from dataclasses import dataclass, field, fields, astuple
from pydantic import BaseModel
from datetime import datetime, date
from typing import Optional, List


class FilmworkModel(BaseModel):
    id: uuid.UUID
    title: str
    # description: str
    # creation_date: date
    # rating: float
    # type: str
    # created_at: datetime
    # updated_at: datetime
    persons: dict[str, list[str]]
    genres: list[str]

