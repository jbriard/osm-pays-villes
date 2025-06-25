import asyncpg
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text
from contextlib import asynccontextmanager
from typing import AsyncGenerator
import logging

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Gestionnaire de connexions à la base de données."""

    def __init__(self, config):
        self.config = config
        self.engine = None
        self.session_factory = None

    async def initialize(self):
        """Initialise le moteur de base de données."""
        db_url = (
            f"postgresql+asyncpg://{self.config.database.user}:"
            f"{self.config.database.password}@{self.config.database.host}:"
            f"{self.config.database.port}/{self.config.database.name}"
        )

        self.engine = create_async_engine(
            db_url,
            echo=False,
            pool_size=20,
            max_overflow=30,
            pool_pre_ping=True
        )

        self.session_factory = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False
        )

        logger.info("Base de données initialisée")

    async def create_tables(self):
        """Crée les tables si elles n'existent pas."""
        from ..models import Base
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Tables créées")

    async def create_indexes(self):
        """Crée les index pour optimiser les performances."""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_countries_osm_id ON countries(osm_id);",
            "CREATE INDEX IF NOT EXISTS idx_countries_alpha2 ON countries(country_code_alpha2);",
            "CREATE INDEX IF NOT EXISTS idx_cities_osm_id ON cities(osm_id);",
            "CREATE INDEX IF NOT EXISTS idx_cities_country_id ON cities(country_id);",
            "CREATE INDEX IF NOT EXISTS idx_cities_coords ON cities(center_lat, center_lng);"
        ]

        # Créer les index en dehors des transactions
        for index_sql in indexes:
            try:
                # Utiliser une connexion directe pour éviter les transactions
                async with self.engine.connect() as conn:
                    # Autocommit pour les index
                    await conn.execute(text(index_sql))
                    await conn.commit()
                logger.info(f"Index créé: {index_sql.split()[5]}")  # Nom de l'index
            except Exception as e:
                logger.warning(f"Erreur création index: {e}")

    @asynccontextmanager
    async def get_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Context manager pour obtenir une session de base de données."""
        async with self.session_factory() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()

    async def close(self):
        """Ferme les connexions."""
        if self.engine:
            await self.engine.dispose()
