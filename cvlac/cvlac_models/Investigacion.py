from cvlac.db_cvlac import Base
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship

class Investigacion(Base):
    
    __tablename__ = 'investigacion'
    id = Column(Integer, primary_key=True)
    idcvlac = Column(String(20), ForeignKey("basico.idcvlac"), nullable=False)
    nombre = Column(String(1000), nullable=True)
    activa = Column(String(100), nullable=True)
    
    basico = relationship('Basico', backref='investigacion')
    
    def __init__(self, **kwargs):
        
        for key, value in kwargs.items():
            setattr(self, key, value)
            
    def __repr__(self):
        return f'Investigacion({self.nombre}, {self.idcvlac}, {self.activa})'
    
    def __str__(self):
        return self.idcvlac