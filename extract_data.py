import os
import praw
import pandas as pd
from datetime import datetime, timedelta
import logging
from pathlib import Path
import yaml
from urllib.parse import urlparse
import time
from prawcore.exceptions import ResponseException

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(config_file='config.yaml'):
    with open(config_file, 'r') as file:
        return yaml.safe_load(file)

# Load configuration
config = load_config()

# Reddit API credentials
try:
    reddit = praw.Reddit(
        client_id=os.getenv('CLIENT_ID'),
        client_secret=os.getenv('CLIENT_SECRET'),
        user_agent=os.getenv('USER_AGENT')
    )
    reddit.user.me()
except Exception as e:
    logging.error(f"Reddit authentication failed: {e}")
    exit(1)

def is_image_url(url):
    image_extensions = ['.jpg', '.jpeg', '.png', '.gif']
    parsed_url = urlparse(url)
    return any(parsed_url.path.lower().endswith(ext) for ext in image_extensions)

def get_media_info(submission):
    media_info = {
        'has_image': False,
        'has_video': False,
        'image_url': None,
        'video_url': None,
        'media_metadata': None
    }

    if submission.url and is_image_url(submission.url):
        media_info['has_image'] = True
        media_info['image_url'] = submission.url

    if hasattr(submission, 'is_video') and submission.is_video:
        media_info['has_video'] = True
        if hasattr(submission, 'media') and submission.media:
            media_info['video_url'] = submission.media.get('reddit_video', {}).get('fallback_url')

    if hasattr(submission, 'is_gallery') and submission.is_gallery:
        media_info['has_image'] = True
        if hasattr(submission, 'media_metadata'):
            media_info['media_metadata'] = submission.media_metadata

    return media_info

def fetch_comments(submission, limit=10):
    comments = []
    submission.comments.replace_more(limit=0)
    sorted_comments = sorted(submission.comments.list(), key=lambda x: x.score, reverse=True)
    
    for comment in sorted_comments:
        if str(comment.author).lower() != 'automoderator':
            comments.append({
                'comment_body': comment.body[:500],
                'comment_score': comment.score,
            })
            if len(comments) >= limit:
                break
    return comments

def fetch_posts_from_subreddits(subreddits, comment_limit, months):
    all_posts = []
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30*months)
    
    for subreddit in subreddits:
        try:
            subreddit_instance = reddit.subreddit(subreddit)
            submissions = subreddit_instance.new(limit=None)
            for submission in submissions:
                submission_date = datetime.fromtimestamp(submission.created_utc)
                if submission_date < start_date:
                    break  # Stop if we've gone past our time limit
                if submission_date > end_date:
                    continue
                
                media_info = get_media_info(submission)
                
                post_data = {
                    'subreddit': subreddit,
                    'post_id': submission.id,
                    'created_utc': submission_date.strftime('%Y-%m-%d %H:%M:%S'),
                    'post_flair': submission.link_flair_text,
                    'title': submission.title,
                    'selftext': submission.selftext[:500],
                    'score': submission.score,
                    'upvote_ratio': submission.upvote_ratio,
                    'num_comments': submission.num_comments,
                    'url': submission.url,
                    'has_image': media_info['has_image'],
                    'has_video': media_info['has_video'],
                    'image_url': media_info['image_url'],
                    'video_url': media_info['video_url'],
                    'is_gallery': hasattr(submission, 'is_gallery') and submission.is_gallery,
                }
                
                if media_info['media_metadata']:
                    post_data['gallery_item_count'] = len(media_info['media_metadata'])
                
                comments = fetch_comments(submission, limit=comment_limit)
                for i, comment in enumerate(comments):
                    for key, value in comment.items():
                        post_data[f'{key}_{i+1}'] = value
                all_posts.append(post_data)
                
                # Add a small delay between requests to respect rate limits
                time.sleep(0.1)
            
            logging.info(f"Fetched posts from r/{subreddit}")
        except ResponseException as e:
            if e.response.status_code == 429:
                logging.warning(f"Rate limit reached for r/{subreddit}. Waiting for 60 seconds before retrying...")
                time.sleep(60)
                # You might want to implement a retry mechanism here
            else:
                logging.error(f"Error fetching posts from r/{subreddit}: {e}")
        except Exception as e:
            logging.error(f"Error fetching posts from r/{subreddit}: {e}")
    
    return all_posts

def main():
    subreddits_list = config['subreddits']
    post_limit = config['post_limit']
    comment_limit = config['comment_limit']
    months = config['months']
    
    logging.info(f"Starting data collection for top {post_limit} posts in the past {months} months from all specified subreddits...")
    all_posts = fetch_posts_from_subreddits(subreddits_list, comment_limit=comment_limit, months=months)
    
    if not all_posts:
        logging.warning("No data collected. Exiting.")
        return
    
    # Sort all posts by score and take top 300
    all_posts.sort(key=lambda x: x['score'], reverse=True)
    top_posts = all_posts[:post_limit]
    
    df = pd.DataFrame(top_posts)
    
    Path("data").mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"data/reddit_top_posts_{timestamp}.csv"
    
    try:
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        logging.info(f"Data saved to {filename}")
        logging.info(f"Total posts collected: {len(df)}")
        logging.info(f"Sample of collected data:\n{df.sample(n=5 if len(df) >= 5 else len(df))}")
    except Exception as e:
        logging.error(f"Error saving data to CSV: {e}")

if __name__ == "__main__":
    main()