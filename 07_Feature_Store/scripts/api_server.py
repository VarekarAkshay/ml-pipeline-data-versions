import os
import sqlite3
import json
import yaml
import pathlib
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uvicorn

# Pydantic models for API requests/responses
class FeatureRequest(BaseModel):
    entity_id: str
    features: List[str]

class BatchFeatureRequest(BaseModel):
    entity_ids: List[str]
    features: List[str]

class HistoricalFeatureRequest(BaseModel):
    entity_df: List[Dict[str, Any]]
    features: List[str]
    timestamp_range: Optional[Dict[str, str]] = None

class FeatureResponse(BaseModel):
    entity_id: str
    features: Dict[str, Any]
    timestamp: str

class FeatureMetadataResponse(BaseModel):
    feature_id: str
    name: str
    description: str
    feature_group: str
    data_type: str
    version: str
    source_table: str
    source_column: str
    tags: List[str]
    statistics: Optional[Dict[str, Any]] = None

def load_config():
    script_folder = pathlib.Path(__file__).parent.resolve()
    config_path = script_folder.parent / 'config_step7.yaml'
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def setup_logging():
    base_folder = pathlib.Path(__file__).parent.parent.resolve()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_dir = base_folder / 'logs'
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"feature_api_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

class FeatureStoreAPI:
    def __init__(self, config):
        self.config = config
        self.base_folder = pathlib.Path(__file__).parent.parent.resolve()
        self.db_path = self.base_folder / config['feature_store']['database_path']
        self.logger = setup_logging()
        
        # Initialize FastAPI app
        self.app = FastAPI(
            title="Feature Store API",
            description="REST API for ML Feature Store",
            version="1.0.0"
        )
        
        # Setup routes
        self.setup_routes()
        
    def get_db_connection(self):
        """Get database connection"""
        return sqlite3.connect(self.db_path)
        
    def setup_routes(self):
        """Setup API routes"""
        
        @self.app.get("/")
        async def root():
            return {"message": "Feature Store API", "version": "1.0.0"}
        
        @self.app.get("/api/v1/health")
        async def health_check():
            """Health check endpoint"""
            try:
                conn = self.get_db_connection()
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM features_metadata")
                feature_count = cursor.fetchone()[0]
                conn.close()
                
                return {
                    "status": "healthy",
                    "timestamp": datetime.now().isoformat(),
                    "feature_count": feature_count,
                    "database_path": str(self.db_path)
                }
            except Exception as e:
                self.logger.error(f"Health check failed: {e}")
                raise HTTPException(status_code=500, detail="Feature store unhealthy")
        
        @self.app.get("/api/v1/features/{feature_name}/entity/{entity_id}")
        async def get_feature(feature_name: str, entity_id: str):
            """Get single feature for entity"""
            try:
                conn = self.get_db_connection()
                cursor = conn.cursor()
                
                # Find feature ID by name
                cursor.execute("""
                    SELECT feature_id FROM features_metadata 
                    WHERE name = ? AND is_active = TRUE
                    ORDER BY version DESC LIMIT 1
                """, (feature_name,))
                
                result = cursor.fetchone()
                if not result:
                    conn.close()
                    raise HTTPException(status_code=404, detail=f"Feature '{feature_name}' not found")
                
                feature_id = result[0]
                
                # Get feature value from online store
                cursor.execute("""
                    SELECT feature_value, last_updated 
                    FROM online_features 
                    WHERE entity_id = ? AND feature_id = ?
                """, (entity_id, feature_id))
                
                result = cursor.fetchone()
                conn.close()
                
                if not result:
                    raise HTTPException(status_code=404, detail=f"Feature value not found for entity {entity_id}")
                
                feature_value = json.loads(result[0])
                last_updated = result[1]
                
                # Log access
                self.log_feature_access(feature_id, entity_id, "online")
                
                return {
                    "entity_id": entity_id,
                    "feature_name": feature_name,
                    "feature_value": feature_value,
                    "last_updated": last_updated
                }
                
            except HTTPException:
                raise
            except Exception as e:
                self.logger.error(f"Error getting feature {feature_name} for entity {entity_id}: {e}")
                raise HTTPException(status_code=500, detail="Internal server error")
        
        @self.app.post("/api/v1/features/batch")
        async def get_batch_features(request: FeatureRequest):
            """Get multiple features for single entity"""
            try:
                conn = self.get_db_connection()
                cursor = conn.cursor()
                
                features_data = {}
                
                for feature_name in request.features:
                    # Find feature ID by name
                    cursor.execute("""
                        SELECT feature_id FROM features_metadata 
                        WHERE name = ? AND is_active = TRUE
                        ORDER BY version DESC LIMIT 1
                    """, (feature_name,))
                    
                    result = cursor.fetchone()
                    if result:
                        feature_id = result[0]
                        
                        # Get feature value
                        cursor.execute("""
                            SELECT feature_value, last_updated 
                            FROM online_features 
                            WHERE entity_id = ? AND feature_id = ?
                        """, (request.entity_id, feature_id))
                        
                        value_result = cursor.fetchone()
                        if value_result:
                            features_data[feature_name] = {
                                "value": json.loads(value_result[0]),
                                "last_updated": value_result[1]
                            }
                            
                            # Log access
                            self.log_feature_access(feature_id, request.entity_id, "batch")
                
                conn.close()
                
                return {
                    "entity_id": request.entity_id,
                    "features": features_data,
                    "request_timestamp": datetime.now().isoformat()
                }
                
            except Exception as e:
                self.logger.error(f"Error getting batch features: {e}")
                raise HTTPException(status_code=500, detail="Internal server error")
        
        @self.app.post("/api/v1/features/historical")
        async def get_historical_features(request: HistoricalFeatureRequest):
            """Get historical features for training"""
            try:
                conn = self.get_db_connection()
                cursor = conn.cursor()
                
                results = []
                
                for entity_data in request.entity_df:
                    entity_id = str(entity_data.get('entity_id'))
                    entity_features = {}
                    
                    for feature_name in request.features:
                        # Find feature ID
                        cursor.execute("""
                            SELECT feature_id FROM features_metadata 
                            WHERE name = ? AND is_active = TRUE
                            ORDER BY version DESC LIMIT 1
                        """, (feature_name,))
                        
                        result = cursor.fetchone()
                        if result:
                            feature_id = result[0]
                            
                            # Get historical value (latest for simplicity)
                            cursor.execute("""
                                SELECT feature_value, timestamp 
                                FROM offline_features 
                                WHERE entity_id = ? AND feature_id = ?
                                ORDER BY timestamp DESC LIMIT 1
                            """, (entity_id, feature_id))
                            
                            value_result = cursor.fetchone()
                            if value_result:
                                entity_features[feature_name] = json.loads(value_result[0])
                    
                    if entity_features:
                        results.append({
                            "entity_id": entity_id,
                            "features": entity_features
                        })
                
                conn.close()
                
                return {
                    "historical_features": results,
                    "request_timestamp": datetime.now().isoformat(),
                    "total_entities": len(results)
                }
                
            except Exception as e:
                self.logger.error(f"Error getting historical features: {e}")
                raise HTTPException(status_code=500, detail="Internal server error")
        
        @self.app.get("/api/v1/metadata/features")
        async def list_features():
            """List all available features"""
            try:
                conn = self.get_db_connection()
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT feature_id, name, description, feature_group_id, data_type, 
                           version, source_table, source_column, tags
                    FROM features_metadata 
                    WHERE is_active = TRUE
                    ORDER BY feature_group_id, name
                """)
                
                features = []
                for row in cursor.fetchall():
                    features.append({
                        "feature_id": row[0],
                        "name": row[1],
                        "description": row[2],
                        "feature_group": row[3],
                        "data_type": row[4],
                        "version": row[5],
                        "source_table": row[6],
                        "source_column": row[7],
                        "tags": json.loads(row[8]) if row[8] else []
                    })
                
                conn.close()
                
                return {
                    "features": features,
                    "total_count": len(features)
                }
                
            except Exception as e:
                self.logger.error(f"Error listing features: {e}")
                raise HTTPException(status_code=500, detail="Internal server error")
        
        @self.app.get("/api/v1/metadata/features/{feature_name}")
        async def get_feature_metadata(feature_name: str):
            """Get feature metadata"""
            try:
                conn = self.get_db_connection()
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT feature_id, name, description, feature_group_id, data_type,
                           version, source_table, source_column, tags, statistics,
                           quality_metrics, creation_date, last_updated
                    FROM features_metadata 
                    WHERE name = ? AND is_active = TRUE
                    ORDER BY version DESC LIMIT 1
                """, (feature_name,))
                
                result = cursor.fetchone()
                conn.close()
                
                if not result:
                    raise HTTPException(status_code=404, detail=f"Feature '{feature_name}' not found")
                
                return {
                    "feature_id": result[0],
                    "name": result[1],
                    "description": result[2],
                    "feature_group": result[3],
                    "data_type": result[4],
                    "version": result[5],
                    "source_table": result[6],
                    "source_column": result[7],
                    "tags": json.loads(result[8]) if result[8] else [],
                    "statistics": json.loads(result[9]) if result[9] else None,
                    "quality_metrics": json.loads(result[10]) if result[10] else None,
                    "creation_date": result[11],
                    "last_updated": result[12]
                }
                
            except HTTPException:
                raise
            except Exception as e:
                self.logger.error(f"Error getting feature metadata: {e}")
                raise HTTPException(status_code=500, detail="Internal server error")
        
        @self.app.get("/api/v1/metadata/features/search")
        async def search_features(tags: Optional[str] = Query(None, description="Comma-separated tags")):
            """Search features by tags"""
            try:
                conn = self.get_db_connection()
                cursor = conn.cursor()
                
                if tags:
                    # Simple tag search - in production, use better full-text search
                    tag_list = [tag.strip() for tag in tags.split(',')]
                    query = """
                        SELECT feature_id, name, description, feature_group_id, data_type,
                               version, tags
                        FROM features_metadata 
                        WHERE is_active = TRUE
                    """
                    
                    cursor.execute(query)
                    all_features = cursor.fetchall()
                    
                    # Filter by tags
                    matching_features = []
                    for feature in all_features:
                        feature_tags = json.loads(feature[6]) if feature[6] else []
                        if any(tag in feature_tags for tag in tag_list):
                            matching_features.append({
                                "feature_id": feature[0],
                                "name": feature[1],
                                "description": feature[2],
                                "feature_group": feature[3],
                                "data_type": feature[4],
                                "version": feature[5],
                                "tags": feature_tags
                            })
                else:
                    # Return all features if no tags specified
                    cursor.execute("""
                        SELECT feature_id, name, description, feature_group_id, data_type,
                               version, tags
                        FROM features_metadata 
                        WHERE is_active = TRUE
                    """)
                    
                    matching_features = []
                    for row in cursor.fetchall():
                        matching_features.append({
                            "feature_id": row[0],
                            "name": row[1],
                            "description": row[2],
                            "feature_group": row[3],
                            "data_type": row[4],
                            "version": row[5],
                            "tags": json.loads(row[6]) if row[6] else []
                        })
                
                conn.close()
                
                return {
                    "features": matching_features,
                    "search_tags": tag_list if tags else None,
                    "total_count": len(matching_features)
                }
                
            except Exception as e:
                self.logger.error(f"Error searching features: {e}")
                raise HTTPException(status_code=500, detail="Internal server error")
        
        @self.app.get("/api/v1/stats")
        async def get_feature_store_stats():
            """Get feature store statistics"""
            try:
                conn = self.get_db_connection()
                cursor = conn.cursor()
                
                # Feature counts by group
                cursor.execute("""
                    SELECT feature_group_id, COUNT(*) 
                    FROM features_metadata 
                    WHERE is_active = TRUE 
                    GROUP BY feature_group_id
                """)
                features_by_group = dict(cursor.fetchall())
                
                # Total feature count
                cursor.execute("SELECT COUNT(*) FROM features_metadata WHERE is_active = TRUE")
                total_features = cursor.fetchone()[0]
                
                # Online store statistics
                cursor.execute("SELECT COUNT(DISTINCT entity_id) FROM online_features")
                entities_with_features = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM online_features")
                total_feature_values = cursor.fetchone()[0]
                
                # Recent access logs
                cursor.execute("""
                    SELECT COUNT(*) FROM feature_access_logs 
                    WHERE access_time >= datetime('now', '-24 hours')
                """)
                requests_last_24h = cursor.fetchone()[0]
                
                conn.close()
                
                return {
                    "total_features": total_features,
                    "features_by_group": features_by_group,
                    "entities_with_features": entities_with_features,
                    "total_feature_values": total_feature_values,
                    "requests_last_24h": requests_last_24h,
                    "timestamp": datetime.now().isoformat()
                }
                
            except Exception as e:
                self.logger.error(f"Error getting stats: {e}")
                raise HTTPException(status_code=500, detail="Internal server error")
        
        @self.app.post("/api/v1/refresh")
        async def refresh_features():
            """Refresh features from source (placeholder)"""
            # In a real implementation, this would trigger the ingestion process
            return {
                "message": "Feature refresh initiated",
                "timestamp": datetime.now().isoformat()
            }
    
    def log_feature_access(self, feature_id: str, entity_id: str, access_type: str):
        """Log feature access for monitoring"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            log_id = f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{entity_id}_{feature_id}"
            
            cursor.execute("""
                INSERT INTO feature_access_logs 
                (log_id, feature_id, entity_id, access_type, access_time, request_source)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
            """, (log_id, feature_id, entity_id, access_type, "api"))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            self.logger.warning(f"Failed to log feature access: {e}")

def create_app():
    """Create FastAPI application"""
    config = load_config()
    api = FeatureStoreAPI(config)
    return api.app

if __name__ == "__main__":
    config = load_config()
    
    # Create API instance
    api = FeatureStoreAPI(config)
    
    # Get serving configuration
    host = config['serving']['api_host']
    port = config['serving']['api_port']
    
    print(f"Starting Feature Store API server at http://{host}:{port}")
    print("API Documentation available at: http://{host}:{port}/docs")
    
    # Start server
    uvicorn.run(
        api.app,
        host=host,
        port=port,
        log_level="info"
    )