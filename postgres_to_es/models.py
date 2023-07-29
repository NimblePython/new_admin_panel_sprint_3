import uuid

from pydantic import BaseModel, Field
from datetime import datetime, date
from typing import Optional


class PersonModel(BaseModel):
    id: uuid.UUID
    name: str


class GenreModel(BaseModel):
    name: str


# todo: сделать актеров и писателей также опциональными
class FilmworkModel(BaseModel):
    id: uuid.UUID
    title: str
    description: str | None
    rating: Optional[float] = None
    type: str = Field(exclude=True)
    created_at: datetime = Field(exclude=True)
    updated_at: datetime = Field(exclude=True)
    actors: Optional[list[PersonModel]] = None
    writers: Optional[list[PersonModel]] = None
    directors: Optional[list[PersonModel]] = None
    genres: list[GenreModel]
    directors_names: Optional[list[str]] = None
    writers_names: Optional[list[str]] = None
    actors_names: Optional[list[str]] = None






