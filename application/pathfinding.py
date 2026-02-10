import heapq
from math import radians, cos, sin, asin, sqrt
from collections import defaultdict

def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    Returns distance in kilometers
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    
    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    km = 6371 * c
    return km

def parse_geo_point(geo_point_str):
    """Parse geo_point string to lat, lon tuple"""
    if not geo_point_str:
        return None
    parts = geo_point_str.strip().split(',')
    if len(parts) != 2:
        return None
    try:
        lat = float(parts[0].strip())
        lon = float(parts[1].strip())
        return (lat, lon)
    except ValueError:
        return None

def build_graph(conn):
    """Build graph from database"""
    cursor = conn.cursor()
    
    # Get all stations with their geo_points
    cursor.execute("SELECT code_uic, libelle, geo_point FROM gares")
    stations = {}
    for code_uic, libelle, geo_point in cursor.fetchall():
        coords = parse_geo_point(geo_point)
        if coords:
            stations[code_uic] = {
                'name': libelle,
                'coords': coords
            }
    
    # Build adjacency list
    graph = defaultdict(list)
    cursor.execute("SELECT gare_origine_code_uic, gare_destination_code_uic FROM lignes")
    
    for origine, destination in cursor.fetchall():
        if origine in stations and destination in stations:
            # Calculate distance
            lat1, lon1 = stations[origine]['coords']
            lat2, lon2 = stations[destination]['coords']
            distance = haversine(lat1, lon1, lat2, lon2)
            
            # Add bidirectional edges
            graph[origine].append((destination, distance))
            graph[destination].append((origine, distance))
    
    return graph, stations

def dijkstra(graph, start, end):
    """Find shortest path using Dijkstra's algorithm"""
    # Priority queue: (distance, current_node, path)
    pq = [(0, start, [start])]
    visited = set()
    distances = {start: 0}
    
    while pq:
        current_dist, current, path = heapq.heappop(pq)
        
        if current in visited:
            continue
            
        visited.add(current)
        
        if current == end:
            return path, current_dist
        
        for neighbor, weight in graph[current]:
            if neighbor not in visited:
                new_dist = current_dist + weight
                if neighbor not in distances or new_dist < distances[neighbor]:
                    distances[neighbor] = new_dist
                    heapq.heappush(pq, (new_dist, neighbor, path + [neighbor]))
    
    return None, None


def find_station_code(conn, station_name):
    """Find station code by name (case-insensitive partial match)"""
    cursor = conn.cursor()
    
    cursor.execute(
        "SELECT code_uic, libelle FROM gares WHERE LOWER(libelle) LIKE LOWER(%s)",
        (f'%{station_name}%',)
    )
    results = cursor.fetchall()
    
    return results



def find_shortest_path(conn, start_name, end_name):
    """Main function to find shortest path between two stations"""
    # Find station codes
    start_matches = find_station_code(conn, start_name)
    end_matches = find_station_code(conn, end_name)
    
    if not start_matches:
        print(f"No station found matching '{start_name}'")
        return
    if not end_matches:
        print(f"No station found matching '{end_name}'")
        return
    
    # Handle multiple matches
    if len(start_matches) > 1:
        print(f"\nMultiple stations found for '{start_name}':")
        for i, (code, name) in enumerate(start_matches, 1):
            print(f"{i}. {name} ({code})")
        choice = int(input("Select station number: ")) - 1
        start_code, start_libelle = start_matches[choice]
    else:
        start_code, start_libelle = start_matches[0]
    
    if len(end_matches) > 1:
        print(f"\nMultiple stations found for '{end_name}':")
        for i, (code, name) in enumerate(end_matches, 1):
            print(f"{i}. {name} ({code})")
        choice = int(input("Select station number: ")) - 1
        end_code, end_libelle = end_matches[choice]
    else:
        end_code, end_libelle = end_matches[0]
    
    # Build graph and find path
    print("\nBuilding graph...")
    graph, stations = build_graph(conn)
    
    print(f"Finding shortest path from '{start_libelle}' to '{end_libelle}'...")
    path, distance = dijkstra(graph, start_code, end_code)

    return_path = [stations[code]['name'] for code in path] if path else None
    
    if path:
        print(f"\n✓ Path found! Total distance: {distance:.2f} km")
        print(f"\nRoute ({len(path)} stations):")
        for i, code in enumerate(path, 1):
            print(f"{i}. {stations[code]['name']}")
        return return_path, distance
    else:
        print(f"\n✗ No path found between these stations")
