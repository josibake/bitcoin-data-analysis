# combiner script

Reads out all the csv files in the bucket, combines them into a single bzipped csv. Not meant to be general purpose, i.e., presumes nix + high ram dev box.

ChatGPT'd script, so overly verbose. Mostly keeping for posterity in case I need to do this again.

## usage

1. Set required fields in the a .env file, e.g., `GOOGLE_APPLICATION_CREDENTIALS=<path/to/json>`
2. `nix run`
