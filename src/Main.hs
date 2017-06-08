{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TemplateHaskell #-}

module Main where

import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.ByteString.Char8 as S8
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString as BS
import Data.Maybe
import Data.Aeson 
import Data.Aeson.Lens
import Data.MultiSet (MultiSet)
import qualified Data.MultiSet as MultiSet
import qualified Data.Set as Set
import Data.List
import Data.Char

import Control.Monad
import Control.Lens 

import Web.Twitter.Conduit hiding (map)
import Web.Twitter.Conduit.Types
import Web.Twitter.Types

import URI.ByteString

import Control.Monad.IO.Class
import Control.Monad.Trans.Resource

import Control.Concurrent.Chan.Unagi
import Control.Concurrent.STM
import Control.Concurrent (forkIO, threadDelay, getNumCapabilities)

import Data.Time.Clock

import Data.Semigroup
import Data.Semigroup.Reducer as Reducer

import System.Environment

import qualified Data.Conduit as C
import qualified Data.Conduit.Binary as CB
import qualified Data.Conduit.List as CL

data Statistics = Statistics { _total :: !Int
                             , _totalWithURL :: !Int
                             , _totalWithMediaURL :: !Int
                             , _totalWithEmoji :: !Int
                             , _elapsedSeconds :: !Int
                             , _urlDomains :: MultiSet BS.ByteString -- Use ByteString for URLs because there's uri-bytestring, but not uri-text
                             , _hashtags :: MultiSet T.Text
                             , _emojis :: MultiSet Char
                             }

makeLenses ''Statistics

instance Monoid Statistics where
  mempty = Statistics 0 0 0 0 0 MultiSet.empty MultiSet.empty MultiSet.empty
  mappend s1 s2 = Statistics { _total = _total s1 + _total s2
                             , _totalWithURL = _totalWithURL s1 + _totalWithURL s2
                             , _totalWithEmoji = _totalWithEmoji s1 + _totalWithEmoji s2
                             , _elapsedSeconds = _elapsedSeconds s1 + _elapsedSeconds s2
                             , _totalWithMediaURL = _totalWithMediaURL s1 + _totalWithMediaURL s2
                             , _urlDomains = _urlDomains s1 `MultiSet.union` _urlDomains s2
                             , _hashtags = _hashtags s1 `MultiSet.union` _hashtags s2
                             , _emojis = _emojis s1 `MultiSet.union` _emojis s2
                             }

instance Semigroup Statistics where
  (<>) = mappend

instance Reducer [Status] Statistics where 
  unit tweets = Statistics { _total = count
                           , _totalWithURL = countWithURL
                           , _totalWithMediaURL = countWithMediaURL
                           , _totalWithEmoji = countWithEmoji
                           , _elapsedSeconds = 1 
                           , _urlDomains = MultiSet.fromList urlDomainList
                           , _hashtags = MultiSet.fromList hashtagList
                           , _emojis = MultiSet.fromList emojiList
                           }
    where
      count = length tweets
      countWithURL = length $ filter (maybe False (not . null . enURLs) . statusEntities) tweets
      countWithMediaURL = length $ filter (maybe False (not . null . enMedia) . statusEntities) tweets
      emojiTexts = filter (not . T.null) . map (T.filter isEmoji . statusText) $ tweets
      countWithEmoji = length emojiTexts
      emojiList = emojiTexts >>= T.unpack
      urlHost = preview ( _Right. authorityL . _Just . authorityHostL . hostBSL ) . parseURI laxURIParserOptions . T.encodeUtf8
      urlDomainList = do tweet <- tweets
                         url <- maybe [] enURLs $ statusEntities tweet
                         maybeToList . urlHost . ueExpanded . entityBody $ url
      hashtagList = do tweet <- tweets
                       hashtag <- maybe [] enHashTags $ statusEntities tweet
                       return . hashTagText . entityBody $ hashtag

main :: IO ()
main = do
  (_in, out) <- newChan
  statsVar <- newTVarIO (mempty :: Statistics)
  -- "the number of Haskell threads that can run truly simultaneously (on separate physical processors) at any given time."
  numCapabilities <- getNumCapabilities 
  -- even if we have only one core, we still want to spin up a worker thread.
  let numWorkerThreads = max 1 (numCapabilities - 1)
  mapM_ (const . forkIO $ generateStats out statsVar) [1..numWorkerThreads]
  forkIO $ printStats statsVar 10 -- print top 10 emoji, hashtags, domains
  readTwitterStream _in

printStats statsVar topKToPrint = 
  forever $ do
    stats <- atomically $ readTVar statsVar
    printStatistics topKToPrint stats
    threadDelay $ 5 * 10^6 -- delay thread 5 seconds.  Might sleep for longer.

-- consumer thread - reads a block from the channel, calculates the statistics, then adds it to the common accumulated statistic
generateStats out statsVar = 
  forever $ do
    tweets <- readChan out
    let tweetStats = unit tweets
    tweetStats `seq` atomically (modifyTVar' statsVar (tweetStats <>))

-- producer thread - reads everything in from twitter and puts it in the channel.
readTwitterStream _in = do
  twInfo <- getTWInfoFromEnv
  mgr <- newManager tlsManagerSettings
  void . runResourceT $ do
    src <- stream twInfo mgr statusesSample
    src C.$=+ filterTweets 
        C.$=+ CL.groupBy currentSecond -- todo: experiment with different block sizes? 
        C.$$+- CL.mapM_ (liftIO . writeChan _in)
  where
    currentSecond status1 status2 = statusCreatedAt status1 == statusCreatedAt status2
    filterTweets = CL.mapMaybe getTweet
      where getTweet (SStatus s) = Just s
            getTweet _ = Nothing 
            

data StatusesSample
statusesSample :: APIRequest StatusesSample StreamingAPI
statusesSample = APIRequestGet "https://stream.twitter.com/1.1/statuses/sample.json" []

 
printStatistics topItemsToDisplay stats = do
  putStrLn $ "Seen a total of " ++ show (stats^.total) ++ " tweets so far." -- ^. is Contral.Lens' view operator
  putStrLn $ "Avg tweets/sec: " ++ show (fromIntegral (stats^.total) / fromIntegral (stats^.elapsedSeconds))
  putStrLn $ "Avg tweets/min: " ++ show ((fromIntegral (stats^.total) * 60) / fromIntegral (stats^.elapsedSeconds))
  putStrLn $ "Avg tweets/hour: " ++ show ((fromIntegral (stats^.total) * 60 * 60) / fromIntegral (stats^.elapsedSeconds))
  putStrLn $ "Percent with URLS: " ++ show (100 * (fromIntegral (stats^.totalWithURL)  / fromIntegral (stats^.total)))
  putStrLn $ "Percent with media: " ++ show (100 * (fromIntegral (stats^.totalWithMediaURL) / fromIntegral (stats^.total)))
  putStrLn $ "Percent with Emoji: " ++ show (100 * (fromIntegral (stats^.totalWithEmoji)  / fromIntegral (stats^.total)))
  putStrLn $ "Top emoji: " ++ show topEmoji 
  putStrLn $ "Top domains: " ++ show topDomains 
  putStrLn $ "Top hashtags: " ++ show topHashtags
  where
    top = take topItemsToDisplay . map fst . sortOn snd . MultiSet.toOccurList
    topDomains = top $ stats^.urlDomains
    topHashtags = top $ stats^.hashtags 
    topEmoji = top  $ stats^.emojis 


isEmoji c = generalCategory c == OtherSymbol && Set.member c emojiSet

-- _emoji copied from https://github.com/iamcal/emoji-data/blob/master/build/emoji_categories.json
allEmoji :: String
allEmoji = "\128512\128515\128516\128513\128518\128517\128514\128092\128188\128083\128374\127746\9730\65039\128054\128049\128045\128057\128048\127869\9917\65039\127936\127944\9918\65039\127934\128424\128433\128434\128377\128476\128189\128190\128191\128192\128252\128247\128248\128249\127909\128253\127902\128222\9742\65039\128223\128224\128250\128251\127897\127898\127899\9201\9202\9200\128368\8987\65039\9203\128225\128267\128268\128161\128294\128367\128465\128738\128184\128181\128180\128182\128183\128176\128179\128142\9878\65039\128295\128296\9874\128736\9935\128297\9881\65039\9939\128299\128163\128298\128481\9876\65039\128737\128684\9904\65039\9905\65039\10060\11093\65039\127462\127465\127462\127476\127462\127470\127462\127478\127462\127468\127462\127479\127462\127474\127462\127484\127462\127482\127462\127481\127462\127487\127463\127480\127463\127469\127463\127465\127463\127463\127463\127486\127463\127466\127463\127487\127463\127471\127463\127474\127463\127481\127463\127476\127463\127478\127463\127462\127463\127484\127463\127479\127470\127476\127483\127468\127463\127475\127463\127468\127463\127467\127463\127470\127464\127483\127472\127469\127464\127474\127464\127462\127470\127464\127472\127486\127464\127467\127481\127465\127464\127473\127464\127475\127464\127485\127464\127464\127464\127476\127472\127474\127464\127468\127464\127465\127464\127472\127464\127479\127464\127470\127469\127479\127464\127482\127464\127484\127464\127486\127464\127487\127465\127472\127465\127471\127465\127474\127465\127476\127466\127464\127466\127468\127480\127483\127468\127478\127466\127479\127466\127466\127466\127481\127466\127482\127467\127472\127467\127476\127467\127471\127467\127470\127467\127479\127468\127467\127477\127467\127481\127467\127468\127462\127468\127474\127468\127466\127465\127466\127468\127469\127468\127470\127468\127479\127468\127473\127468\127465\127468\127477\127468\127482\127468\127481\127468\127468\127468\127475\127468\127484\127468\127486\127469\127481\127469\127475\127469\127472\127469\127482\127470\127480\127470\127475\127470\127465\127470\127479\127470\127478\127470\127466\127470\127474\127470\127473\127470\127481\127471\127474\127471\127477\127884\127471\127466\127471\127476\127472\127487\127472\127466\127472\127470\127485\127472\127472\127484\127472\127468\127473\127462\127473\127483\127473\127463\127473\127480\127473\127479\127473\127486\127473\127470\127473\127481\127473\127482\127474\127476\127474\127472\127474\127468\127474\127484\127474\127486\127474\127483\127474\127473\127474\127481\127474\127469\127474\127478\127474\127479\127474\127482\127486\127481\127474\127485\127467\127474\127474\127465\127474\127464\127474\127475\127474\127466\127474\127480\127474\127462\127474\127487\127474\127474\127475\127462\127475\127479\127475\127477\127475\127473\127475\127464\127475\127487\127475\127470\127475\127466\127475\127468\127475\127482\127475\127467\127474\127477\127472\127477\127475\127476\127476\127474\127477\127472\127477\127484\127477\127480\127477\127462\127477\127468\127477\127486\127477\127466\127477\127469\127477\127475\127477\127473\127477\127481\127477\127479\127478\127462\127479\127466\127479\127476\127479\127482\127479\127484\127463\127473\127480\127469\127472\127475\127473\127464\127477\127474\127483\127464\127484\127480\127480\127474\127480\127481\127480\127462\127480\127475\127479\127480\127480\127464\127480\127473\127480\127468\127480\127485\127480\127472\127480\127470\127480\127463\127480\127476\127487\127462\127468\127480\127472\127479\127480\127480\127466\127480\127473\127472\127480\127465\127480\127479\127480\127487\127480\127466\127464\127469\127480\127486\127481\127484\127481\127471\127481\127487\127481\127469\127481\127473\127481\127468\127481\127472\127481\127476\127481\127481\127481\127475\127481\127479\127481\127474\127481\127464\127481\127483\127482\127468\127482\127462\127462\127466\127468\127463\127482\127480\127483\127470\127482\127486\127482\127487\127483\127482\127483\127462\127483\127466\127483\127475\127484\127467\127466\127469\127486\127466\127487\127474\127487\127484"

emojiSet :: Set.Set Char
emojiSet = Set.fromList allEmoji

-- copied from https://github.com/himura/twitter-conduit/blob/master/sample/common/Common.hs
getOAuthTokens :: IO (OAuth, Credential)
getOAuthTokens = do
    consumerKey <- getEnv' "OAUTH_CONSUMER_KEY"
    consumerSecret <- getEnv' "OAUTH_CONSUMER_SECRET"
    accessToken <- getEnv' "OAUTH_ACCESS_TOKEN"
    accessSecret <- getEnv' "OAUTH_ACCESS_SECRET"
    let oauth = twitterOAuth
            { oauthConsumerKey = consumerKey
            , oauthConsumerSecret = consumerSecret
            }
        cred = Credential
            [ ("oauth_token", accessToken)
            , ("oauth_token_secret", accessSecret)
            ]
    return (oauth, cred)
  where
    getEnv' = (S8.pack <$>) . getEnv

getTWInfoFromEnv :: IO TWInfo
getTWInfoFromEnv = do
    (oa, cred) <- getOAuthTokens
    return $ (setCredential oa cred def)
