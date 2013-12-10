# The Shogun Machine Learning Toolbox (Data Sets)
-------------------------------------------------

### Instructions for developers
------------------------------

Usually there is no need to checkout this module separately.  Just 
checkout (or clone) the source repository https://github.com/shogun-toolbox/shogun
and issue ```git submodule update --init``` in the root directory.
Then, fetch the data files by simply doing ```git submodule update```.

Every new revision must be committed in shogun as well.  After merging
new commits to `shogun-date`, you need to commit the new revision to
the `shogun` repository:

   `cd data && git checkout master && cd ..`
   `git add data && git commit -m "updating revision of data submodule"`
