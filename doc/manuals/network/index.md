# CCTools Network Options

When working in a cloud or HPC environment, you may find complex network
conditions such as multiple network interfaces, firewalls, and other
conditions. The following environment variables can be used to shape the
network behavior of Makeflow, Work Queue, Parrot, Chirp, and other tools, so
as to work correctly in these environments.

## IPV6 Support

IPV6 is supported by all of the CCTools components, however it is not turned
on by default because IPV6 is not reliably deployed at all sites. You can
enable IPV6 support with the `CCTOOLS_IP_MODE` environment variable.

To enable both IPV4 and IPV6 support according to the local system
configuration: (recommended use)

    
    
    export CCTOOLS_IP_MODE=AUTO
    

To enable **only** IPV4 support: (the default)

    
    
    export CCTOOLS_IP_MODE=IPV4
    

To enable **only** IPV6 support: (not recommended; use only for testing IPV6)

    
    
    export CCTOOLS_IP_MODE=IPV6
    

Where it is necessary to combine an address and port together into a single
string, an IPV4 combination looks like this:

    
    
    192.168.0.1:9094
    

But an IPV6 combination looks like this:

    
    
    [1234::abcd]:9094
    

## TCP Port Ranges

When creating a listening TCP port, the CCTools will, by default, pick any
port available on the machine. However, some computing sites set up firewall
rules that only permit connections within certain port ranges. To accommodate
this, set the ` TCP_LOW_PORT` and `TCP_HIGH_PORT` environment variables, and
the CCTools will only use ports within that range.

For example, if your site firewall only allows ports 8000-9000, do this:

    
    
    export TCP_LOW_PORT=8000
    export TCP_HIGH_PORT=9000
    

## TCP Window Size

The performance of TCP connections over wide area links can be significantly
affected by the "window size" used within the kernel. Ideally, the window size
is set to the product of the network bandwidth and latency, and is managed
automatically by the kernel. In certain cases, you may wish to set it manually
with the `TCP_WINDOW_SIZE` environment variable, which gives the window size
in bytes.

For example, to se the window size to 1MB:

    
    
    export TCP_WINDOW_SIZE=1048576
    

## HTTP Proxies

if your computing site requires all HTTP requests to be routed through a
proxy, specify that proxy with the `HTTP_PROXY` environment variable. The
value should be a semi-colon separated list of proxy URLs, in order of
preference. The final entry may be `DIRECT` indicating that a direct
connection should be attempted if all proxy connections fail.

For example:

    
    
    export HTTP_PROXY=http://proxy01.nd.edu:3128;http://proxy02.nd.edu:3129;DIRECT
    

