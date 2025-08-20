### Initialize Git in your local project (if not already done)
git init
### Add the remote GitHub repo
git remote add origin git@github.com:11kainani/data504_fp.git
### Fetch remote branches
git fetch origin
### Check available branches
git branch -r
### Create a local branch that tracks the remote
git checkout -b dev/xavier origin/dev/xavier
### Verify tracking
git branch -vv