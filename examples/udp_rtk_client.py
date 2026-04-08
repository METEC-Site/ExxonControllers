import socket
import time
import threading
from pathlib import Path

from datetime import datetime

baseDir = "./Data"

#directory_path = Path("/path/to/new/recursive/directory")

HOST = '0.0.0.0'
PORT = 13521

RTKList = {
    'RTKFacet1': {
        'HOST': '192.168.108.2',
        },
    'RTKFacet2': {
        'HOST': '192.168.108.3',
        },
    'RTKFacet3': {
        'HOST': '192.168.108.4',
        },
    'RTKPostcard1': {
        'HOST': '192.168.108.5',
        },
    'RTKPostcard2': {
        'HOST': '192.168.108.6',
        },
    }

def start_client():
    # Create threads....
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((HOST, PORT))
    
    for RTK in RTKList:
        RTKList[RTK]['FILE'] = None
        RTKList[RTK]['TIME'] = time.time()
        
    time.sleep(1)
    
    try:
        while True:
            sock.settimeout(0.1)
            try:
                data, address = sock.recvfrom(4096)
                #print(f"UDP Connection Detected to {address} with data: {data}")
            except:
                address = '',''
                pass
                
            # Check if packet received came from a registered device.
            for RTK in RTKList:
                # Check for *any* packet
                ip, host = address
                #print(f"Address vs host: {ip} vs {RTKList[RTK]['HOST']}")
                if ip == RTKList[RTK]['HOST']:
                    # Successfully matched up packet....
                    # Now check if file already exists for this device..
                    
                    if RTKList[RTK]['FILE'] is not None:
                        RTKList[RTK]['FILE'].write(data)
                        RTKList[RTK]['TIME'] = time.time()
                    else:
                        now = datetime.now()
                        filepath = baseDir + '/' + \
                            now.strftime("%y") + '/' + \
                            now.strftime("%m") + '/' + \
                            now.strftime("%d") + '/'
                        iso_timestamp = now.strftime("%Y-%m-%dT%H-%M-%S")
                        Path(filepath).mkdir(parents=True, exist_ok=True)
                        print(f"\nCreating new file for device: {RTK}\n")
                        RTKList[RTK]['FILE'] = open(filepath+RTK+'_'+iso_timestamp+'.ubx', 'ab')
                    
                # Now iterate around other device to see if any were recording
                #   and are no longer... In which case we need to close file...
                
                if time.time() - RTKList[RTK]['TIME'] > 90 and RTKList[RTK]['FILE'] is not None:
                    RTKList[RTK]['FILE'].close()
                    # Reset timer to avoid repeatedly checking every second
                    RTKList[RTK]['TIME'] = time.time()
                    print(f"\nOperating Device no longer detected: {RTK}\n")
                    # We no longer are receiving data, close file, reset
                    RTKList[RTK]['FILE'] = None
                elif time.time() - RTKList[RTK]['TIME'] > 15 and RTKList[RTK]['FILE'] is None:
                    # Reset timer to avoid repeatedly checking every second
                    RTKList[RTK]['TIME'] = time.time()
                    print(f"Device still not detected: {RTK}")
            time.sleep(0.01)
                    
    except KeyboardInterrupt:
        print("Ctrl+C detected. Shutting down listener")
    finally:
        for RTK in RTKList:
            if RTKList[RTK]['FILE'] is not None:
                RTKList[RTK]['FILE'].close()
        print("Gracefully closed all threads")

if __name__ == "__main__":
    start_client()