from __future__ import annotations
from config import *
from processors import ApiNodes, ApiEdges
from repo.postgres import Database
import json
import os

if __name__ == "__main__":
    """Main upserter program."""

    api_header = json.loads(os.getenv("API_HEADER", ""))
    api_endpoint = os.getenv("API_ENDPOINT", "")

    # open the db connection
    use_ssh = (os.getenv("USE_SSH")=="True")
    dsn_connection_str = os.getenv("DB_CONF", "")
    db = Database(use_ssh, dsn_connection_str)

    # list of database views that should be upserted to nodes
    dbview_nodes = [
            'n_advertisingapp_campaign',
            'n_blogapp_post',
            'n_blogcats',
            'n_blogtags',
            'n_collection',
            'n_location_websites'
            ]
    for view in dbview_nodes:
        nodes = ApiNodes(db, 'nodes', api_header, api_endpoint, view)
        nodes.upserter(chunk_by=50)

    # list of database views that should be upserted to node images
    dbview_images = [
            'img_rectanglemd',
            'img_skyscraper'
            ]
    for view in dbview_images:
        nodes = ApiNodes(db, 'images', api_header, api_endpoint, view)
        nodes.upserter(chunk_by=50)

    # # list of database views that should be upserted to node files
    # dbview_files = ['file_<table_name>']
    # for view in dbview_files:
        # nodes = ApiNodes(db, 'files', api_header, api_endpoint, view)
        # nodes.select()
        # nodes.upsert()

    # list of database views that should be upserted to edges
    dbview_edges = [
            'e_campaign_adbanners',
            'e_category_post',
            'e_tag_post',
            'e_website_post'
            ]
    for view in dbview_edges:
        edges = ApiEdges(db, 'edges', api_header, api_endpoint, view)
        edges.upserter(chunk_by=500)

    db.close()
