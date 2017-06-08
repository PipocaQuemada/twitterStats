# twitterStats
Real-time statistics calculated from Twitter's streaming API

# Setting up the Environment

You'll need to add the environment variables `OAUTH_CONSUMER_KEY`, `OAUTH_CONSUMER_SECRET`, `OAUTH_ACCESS_TOKEN`, and `OAUTH_ACCESS_SECRET` before running.  You can get those values by creating a new twitter application at https://apps.twitter.com/, and generating application tokens for it.

# Building & Running

To build and run using Cabal's nix-style local builds, just:

    bash$ cabal new-build
    bash$ ./dist-newstyle/build/twitterStats-0.1.0.0/build/twitterStats/twitterStats
    
It will print the statistics to the command line every 5 seconds.
