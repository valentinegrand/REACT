"""
file crud.py
manage CRUD and adapt model data from db to schema data to api rest
"""

import pandas as pd
from typing import Optional,List
from sqlalchemy.orm import Session
from sqlalchemy import desc, extract, between
from sqlalchemy import func
from fastapi.logger import logger
import models, schemas

# CRUD for Movie objects

#----------------------------------------- MOVIES ---------------------------------------------

########## Fonctions GET 

# Récupérer un movie grâce à son ID 
def get_movie(db: Session, movie_id: int):
    # read from the database (get method read from cache)
    # return object read or None if not found
    db_movie = db.query(models.Movie).filter(models.Movie.id == movie_id).first()
    logger.error(f"Movie retrieved from DB: {db_movie.title}")
    logger.error("director: {}".format( 
              db_movie.director.name if db_movie.director is not None else "no director"))
    logger.error(f"actors: {db_movie.actors}")
    return db_movie

# Récupération de tous les films (limite à 100)
def get_movies(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.Movie).offset(skip).limit(limit).all()

# Récupération des films avec le titre (en entier)
def get_movies_by_title(db: Session, title: str):
    return db.query(models.Movie).filter(models.Movie.title == title).all()

# Récupération des films avec seulement une partie du titre
def get_movies_by_parttitle(db: Session, title: str):
    return db.query(models.Movie).filter(models.Movie.title.like(f'%{title}%')).all()


# Récupération des films avec l'année
def get_movies_by_year(db: Session, year: int):
    return db.query(models.Movie).filter(models.Movie.year == year).all()


# Récupération des films avec un intervalle d'années
def get_movies_by_range_year(db: Session, year_min: Optional[int] = None, year_max: Optional[int] = None):
    if year_min is None and year_max is None:
        return None
    elif year_min is None:
        return db.query(models.Movie).filter(models.Movie.year <= year_max).all()
    elif year_max is None:
        return db.query(models.Movie).filter(models.Movie.year >= year_min).all()
    else:
        return db.query(models.Movie) \
                .filter(
                    models.Movie.year >= year_min,
                    models.Movie.year <= year_max) \
                .order_by(models.Movie.title) \
                .all()


# Récupération des films avec le nom du réalisateur 
def get_movies_by_director_endname(db: Session, endname: str):
    return db.query(models.Movie).join(models.Movie.director)      \
            .filter(models.Star.name.like(f'%{endname}')) \
            .order_by(desc(models.Movie.year))  \
            .all()



# Nombre de films par année
def get_movies_count_by_year(db: Session):
    # NB: func.count() i.e. count(*) en SQL
    return db.query(models.Movie.year, func.count())  \
            .group_by(models.Movie.year)  \
            .order_by(models.Movie.year)  \
            .all()

# Durée du film min, max et moyenne pour chaque année
def get_movies_duration_by_year(db: Session):
    # NB: func.count() i.e. count(*) en SQL
    return db.query(models.Movie.year, func.min(models.Movie.duration),func.max(models.Movie.duration),func.avg(models.Movie.duration))  \
            .group_by(models.Movie.year)  \
            .order_by(models.Movie.year)  \
            .all()

########## Fonction add/create : POST

# Créer un film et l'ajouter à la bdd
def create_movie(db: Session, movie: schemas.MovieCreate):
    # convert schema object from rest api to db model object
    db_movie = models.Movie(title=movie.title, year=movie.year, duration=movie.duration)
    # add in db cache and force insert
    db.add(db_movie)
    db.commit()
    # retreive object from db (to read at least generated id)
    db.refresh(db_movie)
    return db_movie

########## Fonction update : PUT

# Modifier un film et aplliquer dans la bdd
def update_movie(db: Session, movie: schemas.Movie):
    db_movie = db.query(models.Movie).filter(models.Movie.id == movie.id).first()
    if db_movie is None:
        return None
        # update data from db
        db_movie.title = movie.title
        db_movie.year = movie.year
        db_movie.duration = movie.duration
        # validate update in db
        db.commit()
    # return updated object or None if not found
    return db_movie

########## Fonction delete : DELETE

#Supprimer un film dans la bdd
def delete_movie(db: Session, movie_id: int):
     db_movie = db.query(models.Movie).filter(models.Movie.id == movie_id).first()
     if db_movie is None:
         return None
         # delete object from ORM
         db.delete(db_movie)
         # validate delete in db
         db.commit()
     # return deleted object or None if not found
     return db_movie


#----------------------------------------- STARS ---------------------------------------------

########## Fonctions GET 

#Récupérer une star avec son id 
def get_star(db: Session, star_id: int):
    # read from the database (get method read from cache)
    # return object read or None if not found
    return db.query(models.Star).filter(models.Star.id == star_id).first()
    #return db.query(models.Star).get(1)
    #return schemas.Star(id=1, name="Fred")

# Récupérer toutes les stars (limite de 100)
def get_stars(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.Star).offset(skip).limit(limit).all()

# Récupérer une star avec son nom (en entier)
def get_stars_by_name(db: Session, name: str):
    return db.query(models.Star).filter(models.Star.name == name).order_by(models.Star.name).all()

# Récupérer une star avec son nom (peut être partiellement écrit)
def get_stars_by_partname(db: Session, name: str):
    return db.query(models.Star).filter(models.Star.name.like(f'%{name}%')).all()

# Récupérer une star avec sa date de naissance
def get_stars_by_birthyear(db: Session, year: int):
    return db.query(models.Star).filter(extract('year', models.Star.birthdate) == year) \
            .order_by(models.Star.name)  \
            .all()
######### Fonction add/create : POST

# AJouter une star à la bdd
def create_star(db: Session, star: schemas.StarCreate):
    # convert schema object from rest api to db model object
    db_star = models.Star(name=star.name, birthdate=star.birthdate)
    # add in db cache and force insert
    db.add(db_star)
    db.commit()
    # retreive object from db (to read at least generated id)
    db.refresh(db_star)
    return db_star

########## Fonction update : PUT

# Modifier une star de la bdd
def update_star(db: Session, star: schemas.Star):
    db_star = db.query(models.Star).filter(models.Star.id == star.id).first()
        # update data from db
    db_star.name = star.name
    db_star.birthdate = star.birthdate
        # validate update in db
    db.commit()
    # return updated object or None if not found
    return db_star

########## Fonction delete : DELETE   

# Supprimer une star de la BDD
def delete_star(db: Session, star_id: int):
     db_star = db.query(models.Star).filter(models.Star.id == star_id).first()
     if db_star is None:
         return None
         # delete object from ORM
         db.delete(db_star)
         # validate delete in db
         db.commit()
     # return deleted object or None if not found
     return db_star


#----------------------------------------- ASSOCIATION PLAY ---------------------------------------------


######### Fonction GET : GET  (+stats)

# Récupérer le réalisateur avec l'id du film
def get_stars_by_movie_id(db: Session, movie_id: Optional[int] = None):
    movie_director = db.query(models.Movie).filter(models.Movie.id == movie_id).join(models.Movie.director).first()
    if movie_director is not None:
        return movie_director.director
    else:
        return None

# Récupérer le réalisateur avec le titre (partiel ou non) du film
def get_star_director_movie_by_title(db: Session, title: str):
    db_movies = db.query(models.Movie).filter(models.Movie.title.like(f'%{title}%')) \
        .join(models.Movie.director)  
    return [ db_movie.director for db_movie in db_movies ]

# Récupérer le nom des acteurs d'un film avec son titre (partiel ou non)
def get_actors_by_movie_title(db: Session, title: str):
    return db.query(models.Star).join(models.Movie.actors) \
            .filter(models.Movie.title.like(f'%{title}'))   \
            .order_by(models.Star.name) \
            .all()


# Récupération du film avec le nom d'un acteur
def get_movies_by_actor(db: Session, name: str):
    return db.query(models.Movie).join(models.Movie.actors) \
            .filter(models.Star.name.like(f'%{name}'))   \
            .order_by(desc(models.Movie.year))              \
            .all()

# Nombre de films par réalisateur (avec nb de films supérieur à ...)
def get_stats_by_movie_director(db: Session, min_count: int):
    return db.query(models.Star, func.count(models.Movie.id).label("movie_count"))  \
        .join(models.Movie.director)        \
        .group_by(models.Star)  \
        .having(func.count(models.Movie.id) >= min_count) \
        .order_by(desc("movie_count")) \
        .all()

def get_stats_by_movie_actor(db: Session, min_count: int):
    return db.query(models.Star, func.count(models.Movie.id).label("movie_count"),func.min(models.Movie.year).label("first_year_movie"),func.max(models.Movie.year).label("last_year_movie")) \
        .join(models.Movie.actors)        \
        .group_by(models.Star)  \
        .having(func.count(models.Movie.id) >= min_count) \
        .order_by(desc("movie_count")) \
        .all()

########## Fonction add : POST

# Ajouter un acteur à un film dans la bdd
def add_movie_actor(db: Session, movie_id: int, actor_id: int):
    db_movie = get_movie(db=db, movie_id=movie_id)
    db_star =  get_star(db=db, star_id=actor_id)
    if db_movie is None or db_star is None:
        return None
    # update object association
    db_movie.actors.append(db_star)
    # commit transaction : update SQL
    db.commit()
    # return updated object
    return db_movie

########## Fonction update : UPDATE

# Modifier le réalisateur d'un film
def update_movie_director(db: Session, movie_id: int, director_id: int):
    db_movie = get_movie(db=db, movie_id=movie_id)
    db_star =  get_star(db=db, star_id=director_id)
    if db_movie is None or db_star is None:
        return None
    # update object association
    db_movie.director = db_star
    # commit transaction : update SQL
    db.commit()
    # return updated object
    return db_movie



# Modifier la liste des acteurs d'un film
def update_movie_actor(db: Session, movie_id: int, actor_id: List[int]):
    db_movie = get_movie(db=db, movie_id=movie_id)
    db_movie.actors = []
    for i in actor_id : 
        db_star = get_star(db=db, star_id=i)
        if db_movie is None or db_star is None:
            return None
    # update object association 
        db_movie.actors.append(db_star)
    # commit transaction : update SQL
    db.commit()
    # return updated object
    return db_movie











