# Banners

Banners is a low-dependancy library for basic publish and subscribe operations. It was developed by the Treehouse team from 309 SWEG/EDDGE to enable inter-pod message passing within a kubernettes application. The name Banners is inspired by the historical usage of using flags to spread messages over long distances.

Banners is designed to by used as an included library in distributed applications where there are not high performance requirements for detecting new events. Applications requiring  high performance would benefit from a hardened message broker such as Kafka, Redis, or Active MQ. In applications where a user wants a standalone pub/sub microservice, or a direct peer to peer pub/sub, [FastAPI Pub/Sub](https://github.com/permitio/fastapi_websocket_pubsub) may be more appropriate. However, both of these alternatives comes with significant dependency overhead. If little dependency is what you need, Banners may be a good solution.

## Installation

Clone this repository:

```
git clone https://github.com/AFMC-MAJCOM/Banners.git
```

Navigate to the Banners folder and install using PIP

```
cd banners
pip install .
```

Backend implementations with dependencies, such as S3 and Postgres, can be installed optionally.

## Usage

Banners currently supports 3 backend implementations:

1. Local filesystem files
2. S3 file share files
3. PostgreSQL

The following will show basic usage of the library. An interactive notebook of this functionality can be found [here](demos/basic_usage.ipynb).

### Creating a New Banner:

Each banner implementation is derived from a BaseBanner class. Begin using Banners by instantiating the class that best fits your needs. For here, we'll use LocalBanner.

```
banner = LocalBanner()
```


### Wave (publish):

Send a new event by waving a new banner:

```
banner.wave("topic", {"message": "data"})
```

### Watch (subscribe)

Subscribe to an event topic by watching for new banners:

```
def callback_fn(body: dict):
    print("Received message")

banner.watch("topic", callback_fn)
```

### Ignore (unsubscribe)

Stop watching a previously subscribed to topic:

```
banner.ignore("topic")
```
