import urllib.request
import json
import sys

try:
    # Test status
    url = 'http://localhost:8000/api/status'
    with urllib.request.urlopen(url, timeout=5) as res:
        data = json.loads(res.read().decode())
        print('=== Backend Status ===')
        print(f"Graph Built: {data.get('graph_built')}")
        print(f"Records: {data.get('record_count')}")
        print(f"Validated: {data.get('validated')}")
        print(f"Dataset Loaded: {data.get('dataset_loaded')}")
        
        graph_built = data.get('graph_built')
        print(f'\nGraph Built Status: {graph_built}')
        
        if graph_built:
            # Test graph endpoint
            print('\n=== Testing /api/graph ===')
            graph_url = 'http://localhost:8000/api/graph'
            try:
                with urllib.request.urlopen(graph_url, timeout=5) as res2:
                    graph_data = json.loads(res2.read().decode())
                    print(f"Nodes: {len(graph_data.get('nodes', []))}")
                    print(f"Relationships: {len(graph_data.get('relationships', []))}")
                    if len(graph_data.get('nodes', [])) > 0:
                        print(f"Sample node: {graph_data['nodes'][0]}")
            except Exception as e:
                print(f"Graph API Error: {e}")
        else:
            print('\n⚠️ Graph not built yet! Build it from dashboard first.')
            
except Exception as e:
    print(f'Error: {e}')
    sys.exit(1)
