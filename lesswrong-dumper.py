import asyncio
import pandas as pd
from tqdm.asyncio import tqdm
import requests
import pandas as pd
from dateutil.relativedelta import relativedelta
import json
from datetime import datetime
import aiohttp
import asyncio
from time import sleep


URL = "https://www.lesswrong.com/graphql"
FILE_PREFIX = "effectivealtruism"

async def send_requests(url, payloads, headers=None, max_concurrent_requests=3, delay_seconds=1):
    """
    Send payloads to a URL in parallelized POST requests with a concurrency limit and a progress bar.

    :param url: The URL to which the requests are sent.
    :param payloads: A list of payloads for the POST requests.
    :param headers: Optional HTTP headers for the requests.
    :param max_concurrent_requests: The maximum number of concurrent requests.
    :param delay_seconds: A length of time, in seconds, to delay before sending the request
    :return: A list of responses from the requests.
    """

    async def fetch(session, payload):
        """ Helper function to send a POST request"""
        async with semaphore, session.post(url, json=payload, headers=headers) as response:
            result = await response.text()  # or response.json() based on your response content type
            await asyncio.sleep(delay_seconds)
            return result

    # Create a semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(max_concurrent_requests)
    
    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(max_concurrent_requests)

        # Create tasks for all payloads
        tasks = [fetch(session, payload) for payload in payloads]

        # Use tqdm to display progress, updating as tasks are completed
        responses = []
        for task in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc='Sending Requests'):
            response = await task
            responses.append(response)
        return responses

# Synchronous wrapper to call the async function
def send_requests_sync(url, payloads, headers=None, delay_seconds=1):
    return asyncio.run(send_requests(url, payloads, headers, delay_seconds=delay_seconds))

# get comments for posts, combine, and optionally turn into a dataframe
def get_posts_comments(post_ids, to_df=True, delay_seconds=1, chunk_size = 10):
    request_headers = {
        'Content-Type': 'application/json',
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/109.0"
    }
    post_comments_operation ="""{
        "operationName": "multiCommentQuery",
        "variables": {
        "input": {
            "terms": {
            "view": "postCommentsTop",
            "limit": 5000,
            "postId": "wxsLeekp6zwWSuAPL"
            },
            "enableCache": false,
            "enableTotal": true
        }
        }
    }"""

    post_comments_query = "query multiCommentQuery($input: MultiCommentInput) {  comments(input: $input) {    results {      ...CommentsList      __typename    }    totalCount    __typename  }}fragment CommentsList on Comment {  _id  postId  tagId  tag {    slug    __typename  }  relevantTagIds  relevantTags {    ...TagBasicInfo    __typename  }  tagCommentType  parentCommentId  topLevelCommentId  descendentCount  title  contents {    _id    html    plaintextMainText    wordCount    __typename  }  postedAt  repliesBlockedUntil  userId  deleted  deletedPublic  deletedReason  hideAuthor  authorIsUnreviewed  user {    ...UsersMinimumInfo    __typename  }  currentUserVote  currentUserExtendedVote  baseScore  extendedScore  score  voteCount  emojiReactors  af  afDate  moveToAlignmentUserId  afBaseScore  afExtendedScore  suggestForAlignmentUserIds  reviewForAlignmentUserId  needsReview  answer  parentAnswerId  retracted  postVersion  reviewedByUserId  shortform  shortformFrontpage  lastSubthreadActivity  moderatorHat  hideModeratorHat  nominatedForReview  reviewingForReview  promoted  promotedByUser {    ...UsersMinimumInfo    __typename  }  directChildrenCount  votingSystem  isPinnedOnProfile  debateResponse  rejected  rejectedReason  modGPTRecommendation  originalDialogueId  __typename}fragment TagBasicInfo on Tag {  _id  userId  name  shortName  slug  core  postCount  adminOnly  canEditUserIds  suggestedAsFilter  needsReview  descriptionTruncationCount  createdAt  wikiOnly  deleted  isSubforum  noindex  __typename}fragment UsersMinimumInfo on User {  _id  slug  createdAt  username  displayName  profileImageId  previousDisplayName  fullName  karma  afKarma  deleted  isAdmin  htmlBio  jobTitle  organization  postCount  commentCount  sequenceCount  afPostCount  afCommentCount  spamRiskScore  tagRevisionCount  reviewedByUserId  __typename}"
    gql_request_payloads = []
        
    print(f"Creating {len(post_ids)//chunk_size} batch requests with {chunk_size} queries in each request")
    
    # break the queries into chunks. each request will include chunk_size queries
    requests_list = []
    for i in range(0, len(post_ids), chunk_size):
        chunk = post_ids[i:i + chunk_size]
        batch_request_json= []
        for post_id in chunk:
            query_json = json.loads(post_comments_operation)
            query_json['variables']['input']['terms']['postId'] = post_id
            query_json['query'] = post_comments_query
            batch_request_json.append(query_json)
        requests_list.append(batch_request_json)
    
    response_list = send_requests_sync(URL, requests_list,
                                       headers=request_headers,
                                       delay_seconds=delay_seconds)
    
    post_comments_results = []
    for batch_response in response_list:
        for batch_query_result in json.loads(batch_response):
            post_comments_results += batch_query_result['data']['comments']['results']

    if to_df:
        print('Transforming comments json to pandas dataframe')
        return transform_posts_to_df(post_comments_results)
    return post_comments_results   

def get_post_data(post_id, to_json=True):
    request_headers = {
    'Content-Type': 'application/json',
    'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/109.0"
}
    get_post_query = """query singlePostQuery($input: SinglePostInput) {
      post(input: $input) {
        result {
          ...PostsDetails
          __typename
        }
        __typename
      }
    }

    fragment PostsDetails on Post {
      ...PostsListBase
      canonicalSource
      viewCount
      collectionTitle
      canonicalSequenceId
      canonicalBookId
      canonicalSequence {
        _id
        title
        __typename
      }
      canonicalBook {
        _id
        title
        __typename
      }
      podcastEpisode {
        title
        podcast {
          title
          applePodcastLink
          spotifyPodcastLink
          __typename
        }
        episodeLink
        externalEpisodeId
        __typename
      }
      sourcePostRelations {
        _id
        sourcePostId
        sourcePost {
          ...PostsListWithVotes
          __typename
        }
        order
        __typename
      }
      targetPostRelations {
        _id
        sourcePostId
        targetPostId
        targetPost {
          ...PostsListWithVotes
          __typename
        }
        order
        __typename
      }
      __typename
    }

    fragment PostsListBase on Post {
      ...PostsBase
      ...PostsAuthors
      readTimeMinutes
      __typename
    }

    fragment PostsBase on Post {
      ...PostsMinimumInfo
      url
      postedAt
      createdAt
      status
      frontpageDate
      commentCount
      voteCount
      contents {
        _id
        htmlHighlight
        plaintextMainText
        html
        wordCount
        version
        __typename
      }
      score
      debate
      question
      hiddenRelatedQuestion
      userId
      location
      website
      contactInfo
      shortform
      reviewCount
      reviewVoteCount
      positiveReviewVoteCount
      group {
        _id
        name
        organizerIds
        __typename
      }
      __typename
    }

    fragment PostsMinimumInfo on Post {
      _id
      slug
      title
      draft
      shortform
      hideCommentKarma
      af
      currentUserReviewVote {
        _id
        qualitativeScore
        quadraticScore
        __typename
      }
      userId
      coauthorStatuses
      hasCoauthorPermission
      rejected
      debate
      collabEditorDialogue
      __typename
    }

    fragment PostsAuthors on Post {
      user {
        ...UsersMinimumInfo
        __typename
      }
      coauthors {
        ...UsersMinimumInfo
        __typename
      }
      __typename
    }

    fragment UsersMinimumInfo on User {
      _id
      slug
      createdAt
      username
      displayName
      fullName
      organization
      __typename
    }


    fragment PostsListWithVotes on Post {
      ...PostsList
      currentUserVote
      currentUserExtendedVote
      __typename
    }

    fragment PostsList on Post {
      ...PostsListBase
      tagRelevance
      deletedDraft
      contents {
        _id
        htmlHighlight
        wordCount
        version
        __typename
      }
      fmCrosspost
      __typename
    }
    """
    single_post_operation = '''{
      "operationName": "singlePostQuery",
      "variables": {
        "input": {
          "selector": {
            "documentId": "DUMMY"
          }
        },
        "sequenceId": null,
        "batchKey": "singlePost"
      }
    }'''
    op_json = json.loads(single_post_operation)
    op_json['variables']['input']['selector']['documentId'] = post_id
    op_json['query'] = get_post_query
    get_post_response = requests.post(URL, json=op_json, headers=request_headers)

    if to_json:
     return get_post_response.json()
    else:
      return get_post_response
    
def get_posts_in_timeframe(start_date: datetime, end_date:datetime, to_df=True):
    request_headers = {
        'Content-Type': 'application/json',
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/109.0"
    }
    new_timestamp_query = """query multiPostQuery($input: MultiPostInput){
        posts(input: $input) {
          results {
            ...SimplePostInfo
            __typename
          }
          totalCount
          __typename
        }
      }

      fragment SimplePostInfo on Post {
        ...PostsAuthors
        _id
        userId
        url
        title
        postedAt
        createdAt
        reviewCount
        reviewVoteCount
        positiveReviewVoteCount
        location
        commentCount
        website
        group {
          _id
          name
          organizerIds
          __typename
        }
        contents {
          _id
          plaintextMainText
          html
          __typename
        }
        __typename
      }

      fragment PostsAuthors on Post {
        user {
          ...UserSimpleInfo
          __typename
        }
        coauthors {
          ...UserSimpleInfo
          __typename
        }
        __typename
      }

      fragment UserSimpleInfo on User {
        _id
        slug
        createdAt
        username
        displayName
        fullName
        karma
        afKarma
        deleted
        isAdmin
        htmlBio
        jobTitle
        organization
        postCount
        commentCount
        sequenceCount
        __typename
      }
    """

    # get a bunch of posts
    posts_by_timestamp_payload ="""[{
      "operationName": "multiPostQuery",
      "variables": {
        "input": {
          "terms": {
            "limit": 2000,
            "view": "timeframe",
            "filter": "frontpage",
            "sortedBy": "magic",
            "after": "2023-10-01T04:00:00.000Z",
            "before": "2023-10-15T03:59:59.999Z"
          },
          "enableCache": false,
          "enableTotal": true
        }
      },"query": "DUMMY"
    }]"""
    # convert the template into a dictionary
    posts_by_timestamp_payload_json = json.loads(posts_by_timestamp_payload)
    # modify the necessary variables
    posts_by_timestamp_payload_json[0]['variables']['input']['terms']['after'] = start_date.isoformat()
    posts_by_timestamp_payload_json[0]['variables']['input']['terms']['before'] = end_date.isoformat()
    posts_by_timestamp_payload_json[0]['query'] = new_timestamp_query
    posts_by_timestamp_response = requests.post(URL, json=posts_by_timestamp_payload_json, headers=request_headers)
    post_results = posts_by_timestamp_response.json()[0]['data']['posts']['results']
    if to_df:
        return transform_posts_to_df(post_results)
    return post_results

def transform_posts_to_df(post_records_json):
    selected_fields = {'userId',
                       'postId',
                        'postedAt',
                        'url',
                        '_id',
                        'title',
                        'commentCount'}
    short_results = []
    for record in post_records_json:
        new_rec = {}
        for field in selected_fields:
            new_rec[field] = record.get(field)
            
        if record.get('contents') is None:
            new_rec['plaintextMainText'] = ''
            # new_rec['html'] = ''
        else:
            new_rec['plaintextMainText'] = record.get('contents',{}).get('plaintextMainText','')
            # new_rec['html'] = record.get('contents',{}).get('html','')
            
        if record.get('user') is None:
            new_rec['userId'] = ''
        else: 
            new_rec['userId'] = record['user']['_id']
            
        short_results.append(new_rec)

    return pd.DataFrame(short_results)

def export_interval(start_date, end_date, delay_seconds=1):
    
    start_date_str = start_date.strftime('%Y%m%d')  # Formats date as 'YYYYMMDD'
    end_date_str = end_date.strftime('%Y%m%d')      # Formats date as 'YYYYMMDD'
    file_prefix = f"{FILE_PREFIX}_{start_date_str}_to_{end_date_str}"

    # Format dates as strings
    print('fetching posts')
    posts_df = get_posts_in_timeframe(start_date, end_date)
    posts_df['postId'] = posts_df['_id']
    posts_df.to_csv(file_prefix+'_posts.csv', index=False)


    # Select rows where 'commentCount' > 1
    selected_rows = posts_df[posts_df['commentCount'] > 0]  
    # Get the 'userID' values from these rows
    post_ids = selected_rows['postId'].tolist()
    # COMMENTS
    # dfs = get_posts_comments(posts_df['postId'])
    print('fetching comments')
    
    comments_df = get_posts_comments(post_ids, delay_seconds=delay_seconds)
    comments_df.to_csv(file_prefix+'_comments.csv', index=False)

def get_user_data(user_ids, chunk_size=3, max_concurrent_requests=3, delay_seconds=1, to_df=True):
    request_headers = {
        'Content-Type': 'application/json',
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/109.0"
    }

    single_user_query_template = """
        {
        "operationName": "singleUserQuery",
        "variables": {
            "input": {
                "selector": {
                    "documentId": "DUMMY"
                }
            }
        },
        "query": "query singleUserQuery($input: SingleUserInput) {  user(input: $input) {    result {      ...SunshineUsersList      __typename    }    __typename  }}fragment SunshineUsersList on User {  ...UsersMinimumInfo  karma  htmlBio  website  createdAt  email  emails  commentCount  maxCommentCount  postCount  maxPostCount  voteCount  smallUpvoteCount  bigUpvoteCount  smallDownvoteCount  bigDownvoteCount  banned  reviewedByUserId  reviewedAt  signUpReCaptchaRating  mapLocation  needsReview  sunshineNotes  sunshineFlagged  postingDisabled  allCommentingDisabled  commentingOnOtherUsersDisabled  conversationsDisabled  snoozedUntilContentCount  nullifyVotes  deleteContent  moderatorActions {    ...ModeratorActionDisplay    __typename  }  usersContactedBeforeReview  associatedClientIds {    clientId    firstSeenReferrer    firstSeenLandingPage    userIds    __typename  }  altAccountsDetected  voteReceivedCount  smallUpvoteReceivedCount  bigUpvoteReceivedCount  smallDownvoteReceivedCount  bigDownvoteReceivedCount  recentKarmaInfo  lastNotificationsCheck  __typename}fragment UsersMinimumInfo on User {  _id  slug  createdAt  username  displayName  profileImageId  previousDisplayName  fullName  karma  afKarma  deleted  isAdmin  htmlBio  jobTitle  organization  postCount  commentCount  sequenceCount  afPostCount  afCommentCount  spamRiskScore  tagRevisionCount  reviewedByUserId  __typename}fragment ModeratorActionDisplay on ModeratorAction {  _id  user {    ...UsersMinimumInfo    __typename  }  userId  type  active  createdAt  endedAt  __typename}"
        }
        """
    
    print(f"Creating {len(user_ids)//chunk_size} batch requests with {chunk_size} queries in each request")
    requests_list = []
    for i in range(0, len(user_ids), chunk_size):
        chunk = user_ids[i:i + chunk_size]
        batch_request_json= []
        for user_id in chunk:
            query_json = json.loads(single_user_query_template)
            query_json['variables']['input']['selector']['documentId'] = user_id
            batch_request_json.append(query_json)
        requests_list.append(batch_request_json)
    user_raw_responses = send_requests_sync(URL, requests_list, headers=request_headers, delay_seconds=delay_seconds)
    
    # each response is a list of batch_responses
    # each batch response is a list of query responses
    # each query response contains results, which is a list of dicts
    user_results = []
    for batch_response in user_raw_responses:
        for user_query_result in json.loads(batch_response):
            user_results.append(user_query_result['data']['user']['result'])
    if to_df:
        print('Transforming comments json to pandas dataframe')
        return transform_posts_to_df(user_results)
    return user_results   

def main():
    start_year = 2022
    start_month = 1
    num_months = 3
    
    test_user_ids = [
        "YaNNYeR5HjKLDBefQ",
        "r38pkCm7wF4M44MDQ",
        "kdeMdATaSc2MZKmdH",
        "X4tKAZja8zaHT6aLu",
        "xSfc2APSi8WzFxp7i",
        "KCExMGwS2ETzN3Ksr",
    ]
    
    # TODO load dataframes from comment/post files, concat into a dataframe
    # TODO extract and de-duplicate userID's
    # get_user_data(test_user_ids)
    
    # for i in range(num_months):
    #     start_date = datetime(start_year,start_month,1,0,0,0,0) + relativedelta(months=i)
    #     end_date = start_date + relativedelta(months=1)
    #     print(
    #         f"exporting posts and comments for period between "
    #         f"{start_date.isoformat()} and {end_date.isoformat()}"
    #     )
    #     export_interval(start_date, end_date)
    user_data = get_user_data(test_user_ids)
    print(user_data)

if __name__ == '__main__':
    main()