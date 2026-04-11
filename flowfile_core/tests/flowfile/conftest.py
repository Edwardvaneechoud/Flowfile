import pytest
from sqlalchemy import Boolean, Column, Float, Integer, String, Text, create_engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

Base = declarative_base()


class Movie(Base):
    __tablename__ = "movies"
    id = Column(Integer, primary_key=True)
    title = Column(String(255), nullable=False)
    rating = Column(Float)
    votes = Column(Integer)
    description = Column(Text)
    is_active = Column(Boolean, default=True)


class Actor(Base):
    __tablename__ = "actors"
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    birth_year = Column(Integer)


@pytest.fixture
def sqlite_db(tmp_path):
    """Create a temporary SQLite database with sample data."""
    db_path = str(tmp_path / "test.db")
    connection_string = f"sqlite:///{db_path}"
    engine = create_engine(connection_string)
    Base.metadata.create_all(engine)

    session_factory = sessionmaker(bind=engine)
    session: Session = session_factory()

    movies = [
        Movie(id=1, title="The Matrix", rating=8.7, votes=1800000, description="A sci-fi classic", is_active=True),
        Movie(id=2, title="Inception", rating=8.8, votes=2200000, description="Mind-bending thriller", is_active=True),
        Movie(id=3, title="The Shawshank Redemption", rating=9.3, votes=2500000, description="Drama", is_active=True),
    ]
    actors = [
        Actor(id=1, name="Keanu Reeves", birth_year=1964),
        Actor(id=2, name="Leonardo DiCaprio", birth_year=1974),
        Actor(id=3, name="Morgan Freeman", birth_year=1937),
    ]

    session.add_all(movies + actors)
    session.commit()
    session.close()
    engine.dispose()

    yield db_path
