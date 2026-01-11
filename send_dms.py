import tweepy
import time
import csv
from datetime import datetime
import random

class TwitterDMSender:
    def __init__(self, api_key, api_secret, access_token, access_token_secret):
        """Initialize Twitter API client"""
        auth = tweepy.OAuthHandler(api_key, api_secret)
        auth.set_access_token(access_token, access_token_secret)
        self.api = tweepy.API(auth, wait_on_rate_limit=True)
        self.client = tweepy.Client(
            consumer_key=api_key,
            consumer_secret=api_secret,
            access_token=access_token,
            access_token_secret=access_token_secret,
            wait_on_rate_limit=True
        )
        
    def personalize_message(self, template, username, custom_fields=None):
        """Personalize message with user data"""
        message = template.replace("{username}", username)
        if custom_fields:
            for key, value in custom_fields.items():
                message = message.replace(f"{{{key}}}", str(value))
        return message
    
    def send_dm(self, username, message):
        """Send a DM to a user"""
        try:
            # Get user ID from username
            user = self.client.get_user(username=username)
            if user.data:
                user_id = user.data.id
                # Send DM
                self.client.create_direct_message(
                    participant_id=user_id,
                    text=message
                )
                print(f"✓ Sent DM to @{username}")
                return True
            else:
                print(f"✗ User @{username} not found")
                return False
        except tweepy.TweepyException as e:
            print(f"✗ Error sending to @{username}: {str(e)}")
            return False
    
    def send_bulk_dms(self, recipients, message_template, 
                      delay_min=60, delay_max=120, 
                      batch_size=10, batch_delay=300):
        """
        Send DMs to multiple recipients with rate limiting
        
        Args:
            recipients: List of dicts with 'username' and optional custom fields
            message_template: Message template with {username} and other placeholders
            delay_min: Minimum delay between messages (seconds)
            delay_max: Maximum delay between messages (seconds)
            batch_size: Number of messages before taking a longer break
            batch_delay: Delay after each batch (seconds)
        """
        total = len(recipients)
        successful = 0
        failed = 0
        
        print(f"\nStarting to send {total} DMs...")
        print(f"Rate limiting: {delay_min}-{delay_max}s between messages")
        print(f"Batch: {batch_size} messages, then {batch_delay}s pause\n")
        
        for i, recipient in enumerate(recipients, 1):
            username = recipient.get('username', '').replace('@', '')
            custom_fields = {k: v for k, v in recipient.items() if k != 'username'}
            
            # Personalize message
            message = self.personalize_message(message_template, username, custom_fields)
            
            print(f"[{i}/{total}] Sending to @{username}...")
            
            # Send DM
            if self.send_dm(username, message):
                successful += 1
            else:
                failed += 1
            
            # Rate limiting
            if i < total:  # Don't delay after last message
                if i % batch_size == 0:
                    print(f"\n⏸  Batch complete. Pausing for {batch_delay}s...\n")
                    time.sleep(batch_delay)
                else:
                    delay = random.randint(delay_min, delay_max)
                    print(f"   Waiting {delay}s...\n")
                    time.sleep(delay)
        
        # Summary
        print("\n" + "="*50)
        print(f"SUMMARY")
        print(f"Total: {total} | Successful: {successful} | Failed: {failed}")
        print("="*50)


def load_recipients_from_csv(filepath):
    """Load recipients from CSV file"""
    recipients = []
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            recipients.append(row)
    return recipients


# USAGE EXAMPLE
if __name__ == "__main__":
    # Twitter API credentials (get from https://developer.twitter.com)
    API_KEY = "your_api_key_here"
    API_SECRET = "your_api_secret_here"
    ACCESS_TOKEN = "your_access_token_here"
    ACCESS_TOKEN_SECRET = "your_access_token_secret_here"
    
    # Initialize sender
    sender = TwitterDMSender(API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    
    # Option 1: Manual list of recipients
    recipients = [
        {"username": "user1", "name": "John"},
        {"username": "user2", "name": "Jane"},
        {"username": "user3", "name": "Bob"}
    ]
    
    # Option 2: Load from CSV file
    # recipients = load_recipients_from_csv('recipients.csv')
    # CSV format: username,name,custom_field
    
    # Message template (use {username} and any other fields from recipient dict)
    message_template = """Hi @{username}!

I wanted to reach out because I think you'd be interested in [your topic].

[Your personalized message here]

Best regards!"""
    
    # Send DMs with conservative rate limiting
    sender.send_bulk_dms(
        recipients=recipients,
        message_template=message_template,
        delay_min=60,      # 1 minute minimum between DMs
        delay_max=120,     # 2 minutes maximum between DMs
        batch_size=10,     # Send 10 DMs then take a break
        batch_delay=300    # 5 minute break after each batch
    )