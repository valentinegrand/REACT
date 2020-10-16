from typing import List, Optional, Set, Tuple
import logging

from fastapi import Depends, FastAPI, HTTPException
from fastapi.logger import logger as fastapi_logger
from sqlalchemy.orm import Session

import crud, models, schemas
from database import SessionLocal, engine

models.Base.metadata.create_all(bind=engine)


app = FastAPI()

logger = logging.getLogger("uvicorn")
fastapi_logger.handlers = logger.handlers
fastapi_logger.setLevel(logger.level)
logger.error("API Started")


# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

'''
@app.post("/movies/")
def create_movie(movie: schemas.MovieCreate, db: Session = Depends(get_db)):
    return False
'''


#----------------------------------------- MOVIES ---------------------------------------------

########## App GET 


@app.get("/movies/", response_model=List[schemas.Movie])
def read_movies(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    # read items from database
    movies = crud.get_movies(db, skip=skip, limit=limit)
    # return them as json
    return movies

@app.get("/movies/by_id/{movie_id}", response_model=schemas.MovieDetail)
def read_movie(movie_id: int, db: Session = Depends(get_db)):
    db_movie = crud.get_movie(db, movie_id=movie_id)
    if db_movie is None:
        raise HTTPException(status_code=404, detail="Movie to read not found")
    return db_movie

### Pour pouvoir gérer les titres compliqués on ne met pas de {title} et on laisse postman gérer les caractères : by_title?title=...
@app.get("/movies/by_title", response_model=List[schemas.Movie])
def read_movies_by_title(title: str, db: Session = Depends(get_db)):
    # read items from database
    movies = crud.get_movies_by_title(db=db, title=title)
    # return them as json
    # return items
    return movies

@app.get("/movies/by_parttitle", response_model=List[schemas.Movie])
def read_movies_by_parttitle(title: str, db: Session = Depends(get_db)):
    # read items from database
    movies = crud.get_movies_by_parttitle(db=db, title=title)
    # return them as json
    return movies

@app.get("/movies/by_year/{year}", response_model=List[schemas.Movie])
def read_movies_by_year(year: Optional[int], db: Session = Depends(get_db)):
    # read items from database
    movies = crud.get_movies_by_year(db=db, year=year)
    # return them as json
    return movies



@app.get("/movies/by_range_year", response_model=List[schemas.Movie])
def read_movies_by_range_year(year_min: Optional[int] = None,
                           year_max: Optional[int] = None,
                           db: Session = Depends(get_db)):
    # read items from database
    movies = crud.get_movies_by_range_year(db=db, year_min=year_min, year_max=year_max)
    if movies is None:
        raise HTTPException(status_code=404, detail="Item for empty range price not found")
    # return them as json
    return movies




@app.get("/movies/count_by_year")
def read_count_movies_by_year(db: Session = Depends(get_db)) -> List[Tuple[int, int]]:
    return crud.get_movies_count_by_year(db=db)

@app.get("/movies/duration_by_year")
def read_count_movies_min_duration_by_year(db: Session = Depends(get_db)) -> List[Tuple[int, int,int,int]]:
    return crud.get_movies_duration_by_year(db=db)

########## POST

@app.post("/movies/", response_model=schemas.Movie)
def create_user(movie: schemas.MovieCreate, db: Session = Depends(get_db)):
    # receive json item without id and return json item from database with new id
    return crud.create_movie(db=db, movie=movie)

######### PUT

@app.put("/movies/", response_model=schemas.Movie)
def update_movie(movie: schemas.Movie, db: Session = Depends(get_db)):
    db_movie = crud.update_movie(db, movie=movie)
    if db_movie is None:
        raise HTTPException(status_code=404, detail="Item to update not found")
    return db_movie

######### DELETE

@app.delete("/movies/{movie_id}", response_model=schemas.Movie)
def delete_movie(movie_id: int, db: Session = Depends(get_db)):
    db_movie = crud.delete_movie(db, movie_id=movie_id)
    if db_movie is None:
        raise HTTPException(status_code=404, detail="Item to delete not found")
    return db_movie

#----------------------------------------- STARS ---------------------------------------------

########## App GET 

@app.get("/stars", response_model=List[schemas.Star])
def read_stars(skip: Optional[int] = 0, limit: Optional[int] = 100, db: Session = Depends(get_db)):
    # read items from database
    stars = crud.get_stars(db, skip=skip, limit=limit)
    # return them as json
    return stars

@app.get("/stars/by_id/{star_id}", response_model=schemas.Star)
def read_star(star_id: int, db: Session = Depends(get_db)):
    db_star = crud.get_star(db, star_id=star_id)
    if db_star is None:
        raise HTTPException(status_code=404, detail="Star to read not found")
    return db_star


@app.get("/stars/by_name", response_model=List[schemas.Star])
def read_stars_by_name(name: Optional[str] = None, db: Session = Depends(get_db)):
    # read items from database
    star = crud.get_stars_by_name(db=db, name=name)
    # return them as json
    return star

@app.get("/stars/by_partname/{name}", response_model=List[schemas.Star])
def read_stars_by_partname(name: Optional[str] = None, db: Session = Depends(get_db)):
    # read items from database
    star = crud.get_stars_by_partname(db=db, name=name)
    # return them as json
    return star

@app.get("/stars/by_birthdate", response_model=List[schemas.Star])
def read_star_by_birthyear(year: Optional[int], db: Session = Depends(get_db)):
    # read items from database
    star = crud.get_stars_by_birthyear(db=db, year=year)
    # return them as json
    return star

 
 
@app.get("/stars/stats_movie_by_director")
def read_stats_movie_by_director(minc: Optional[int] = 10, db: Session = Depends(get_db)):
    return crud.get_stats_by_movie_director(db=db, min_count=minc)


@app.get("/stars/stats_movie_by_actor")
def read_stats_movie_by_actor(minc: Optional[int] = 10, db: Session = Depends(get_db)):
    return crud.get_stats_by_movie_actor(db=db, min_count=minc)

########## App POST
    
@app.post("/stars/", response_model=schemas.Star)
def create_user(star: schemas.StarCreate, db: Session = Depends(get_db)):
    # receive json item without id and return json item from database with new id
    return crud.create_star(db=db, star=star)

######## App PUT

@app.put("/stars/", response_model=schemas.Star)
def update_star(star: schemas.Star, db: Session = Depends(get_db)):
    db_star = crud.update_star(db, star=star)
    if db_star is None:
        raise HTTPException(status_code=404, detail="Item to update not found")
    return db_star

######### App DELETE

@app.delete("/stars/{star_id}", response_model=schemas.Star)
def delete_star(star_id: int, db: Session = Depends(get_db)):
    db_star = crud.delete_star(db, star_id=star_id)
    if db_star is None:
        raise HTTPException(status_code=404, detail="Item to delete not found")
    return db_star


#----------------------------------------- ASSOCIATION PLAY ---------------------------------------------

########## App GET

@app.get("/movies/by_director", response_model=List[schemas.Movie])
def read_movies_by_director(n: str, db: Session = Depends(get_db)):
    return crud.get_movies_by_director_endname(db=db, endname=n)

@app.get("/movies/by_actor", response_model=List[schemas.Movie])
def read_movies_by_actor(name: str, db: Session = Depends(get_db)):
    return crud.get_movies_by_actor(db=db, name=name)

@app.get("/stars/by_movie_id/{movie_id}", response_model=schemas.Star)
def read_star(movie_id: int, db: Session = Depends(get_db)):
    director = crud.get_stars_by_movie_id(db=db, movie_id=movie_id)
    if director is None:
        raise HTTPException(status_code=404, detail="Star to read not found")
    return director

@app.get("/stars/by_movie_title", response_model=List[schemas.Star])
def read_stars_by_movie_directed_title(title: str, db: Session = Depends(get_db)):
    return crud.get_star_director_movie_by_title(db=db, title=title)    
 

@app.get("/stars/actors_by_title", response_model=List[schemas.Star])
def read_stars_by_movie_played_title(title: str, db: Session = Depends(get_db)):
    return crud.get_actors_by_movie_title(db=db, title=title)   


######### App POST

@app.post("/movies/add_actor", response_model=schemas.MovieDetail)
def add_movie_actor(mid: int, sid: int, db: Session = Depends(get_db)):
    db_movie = crud.add_movie_actor(db=db, movie_id=mid, actor_id=sid)
    if db_movie is None:
        raise HTTPException(status_code=404, detail="Movie or Star not found")
    return db_movie

########## App PUT

@app.put("/movies/director", response_model=schemas.MovieDetail)
def update_movie_director(mid: int, sid: int, db: Session = Depends(get_db)):
    db_movie = crud.update_movie_director(db=db, movie_id=mid, director_id=sid)
    if db_movie is None:
        raise HTTPException(status_code=404, detail="Movie or Star not found")
    return db_movie


@app.put("/movies/update_actors", response_model=schemas.MovieDetail)
def update_movie_actors(mid: int, sids: List[int], db: Session = Depends(get_db)):
    db_movie = crud.update_movie_actor(db=db, movie_id=mid, actor_id=sids)
    if db_movie is None:
        raise HTTPException(status_code=404, detail="Movie or Star not found")
    return db_movie
    