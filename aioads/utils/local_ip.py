"""
Module provides a method to get the ip address of the client (this).
"""

import socket


def get_local_ip(target_network: str = "255.255.255.254") -> str:
    """
    Get the local ip address of a interface. 
    If we have multiple interfaces, we can find the ip by specifying a target network. 
    The target network can be any ip address that we want to reach and get the outgoing ip. 
    The target_network address don't has to be reachable, 
    as we only want to get the outgoing ip address for that network.
    By default we use the address "255.255.255.254"
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(0)
    try:
        s.connect((target_network, 1))
        ip = s.getsockname()[0]
    except Exception as ex:
        raise RuntimeError("Could not determine local IP address") from ex
    finally:
        s.close()
    return ip
