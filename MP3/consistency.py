"""
Consistency Manager
Handles merge operations and consistency guarantees
"""

import threading
import time

class ConsistencyManager:
    def __init__(self, node, ring, storage, network):
        self.node = node
        self.ring = ring
        self.storage = storage
        self.network = network
        
        # Background merge
        self.merge_interval = 60  # 60 seconds
        self.merge_thread = None
        self.merge_running = False
    
    def start_background_merge(self):
        """Start background merge process"""
        self.merge_running = True
        self.merge_thread = threading.Thread(target=self._background_merge_loop)
        self.merge_thread.daemon = True
        self.merge_thread.start()
    
    def stop_background_merge(self):
        """Stop background merge process"""
        self.merge_running = False
        if self.merge_thread:
            self.merge_thread.join()
    
    def _background_merge_loop(self):
        """Background merge loop"""
        while self.merge_running:
            time.sleep(self.merge_interval)
            
            # Merge all local files
            files = self.storage.list_files()
            for filename in files:
                try:
                    self.merge(filename)
                except Exception as e:
                    print(f"Background merge error for {filename}: {e}")
    
    def merge(self, filename):
        """Merge replicas of a file"""
        start_time = time.time()
        
        # Get all replicas
        replicas = self.ring.get_replicas_for_file(filename)
        
        # Step 1: Collect block metadata from all replicas
        all_metadata = self.collect_block_metadata(replicas, filename)
        
        # Step 2: Determine canonical order
        canonical_order = self.determine_canonical_order(all_metadata)
        
        # Step 3: Create merge plan
        merge_plan = self.create_merge_plan(canonical_order)
        
        # Step 4: Distribute merge plan to replicas
        self.distribute_merge_plan(replicas, filename, merge_plan)
        
        elapsed = time.time() - start_time
        print(f"✓ Merge completed for {filename} in {elapsed:.3f}s")
        
        return True
    
    def collect_block_metadata(self, replicas, filename):
        """Collect block metadata from all replicas"""
        all_metadata = {}
        
        for replica in replicas:
            message = {
                'type': 'MERGE_REQUEST',
                'action': 'GET_METADATA',
                'filename': filename
            }
            
            try:
                response = self.network.send_request(replica, message)
                if response and response.get('success'):
                    all_metadata[replica] = response['metadata']
            except Exception as e:
                print(f"Error getting metadata from {replica}: {e}")
        
        return all_metadata
    
    def determine_canonical_order(self, all_metadata):
        """Determine canonical block order"""
        # Merge all blocks from all replicas
        all_blocks = []
        for replica, metadata in all_metadata.items():
            all_blocks.extend(metadata)
        
        # Sort by: client_id, then sequence number
        # This ensures per-client ordering
        canonical = sorted(all_blocks, key=lambda b: (b['client_id'], b['sequence']))
        
        return canonical
    
    def create_merge_plan(self, canonical_order):
        """Create merge plan"""
        return {
            'blocks': canonical_order
        }
    
    def distribute_merge_plan(self, replicas, filename, merge_plan):
        """Distribute merge plan to all replicas"""
        for replica in replicas:
            message = {
                'type': 'MERGE_REQUEST',
                'action': 'APPLY_PLAN',
                'filename': filename,
                'plan': merge_plan
            }
            
            try:
                response = self.network.send_request(replica, message)
                if not response or not response.get('success'):
                    print(f"Failed to apply merge plan to {replica}")
            except Exception as e:
                print(f"Error sending merge plan to {replica}: {e}")
    
    def handle_merge_request(self, request):
        """Handle merge request"""
        action = request['action']
        filename = request['filename']
        
        if action == 'GET_METADATA':
            # Return block metadata
            blocks = self.storage.get_file(filename)
            if blocks is None:
                return {'success': False}
            
            metadata = [
                {
                    'client_id': b.client_id,
                    'sequence': b.sequence,
                    'timestamp': b.timestamp,
                    'size': b.size
                }
                for b in blocks
            ]
            
            return {'success': True, 'metadata': metadata}
        
        elif action == 'APPLY_PLAN':
            # Apply merge plan
            plan = request['plan']
            
            # TODO: Request missing blocks from other replicas
            # TODO: Reorder blocks according to plan
            
            return {'success': True}
        
        return {'success': False}