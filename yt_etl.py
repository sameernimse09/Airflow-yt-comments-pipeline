import os
import pandas as pd
import googleapiclient.discovery

def process_comments(response_items):
    comments = []
    for comment in response_items:
        author = comment['snippet']['topLevelComment']['snippet']['authorDisplayName']
        comment_text = comment['snippet']['topLevelComment']['snippet']['textOriginal']
        publish_time = comment['snippet']['topLevelComment']['snippet']['publishedAt']
        comment_info = {'author': author, 
                        'comment': comment_text, 
                        'published_at': publish_time}
        comments.append(comment_info)
    print(f'Finished processing {len(comments)} comments.')
    return comments

def run_yt_comments():
    # Disable OAuthlib's HTTPS verification when running locally.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"
    DEVELOPER_KEY = "########################################"  # Replace with your API key

    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=DEVELOPER_KEY, cache_discovery=False)

    try:
        request = youtube.commentThreads().list(
            part="snippet,replies",
            videoId="VAGZGQg31hs",  
            maxResults=100  
        )
        response = request.execute()
        print("API Response:",response)
    #Process the comments from the response
        if 'items' in response:
            comments = process_comments(response['items'])
            df = pd.DataFrame(comments)
            print (df)
            df.to_csv("s3://#############################/yt_comments.csv",index=False) # Replace with your s3 bucket link
            print("Comments saved to yt_comment.csv")
        else:
            print("No comments found")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__=="__main__":
    run_yt_comments()