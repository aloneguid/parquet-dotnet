# Contributing to Parquet.Net

First off, thank you for considering contributing to Parquet.Net. It's people like you that make it such a great library.

## 1. Where do I go from here?

If you've noticed a bug or have a question see if someone else in the community has already [created a ticket](https://github.com/elastacloud/parquet-dotnet/issues?q=something).
If not, go ahead and [make one](https://github.com/elastacloud/parquet-dotnet/issues/new)!

## 2. Fork & create a branch

If this is something you think you can fix, then
[fork parquet-dotnet](https://help.github.com/articles/fork-a-repo)
and create a branch with a descriptive name.

If you need a started guide to understand Parquet format please read [this guide](../doc/parquet-getting-started.md)

A good branch name would be (where issue #325 is the ticket you're working on):

```sh
git checkout -b 325-add-japanese-translations
```

## 3. Get the fix working

Make the changes and make sure they are working locally. In case of code changes we strongly advise to create a unit/integration test before finishing your work. We will not accept any code (unless it's a trivial change or documentation) without any tests because we won't know if it fixes anything at all.

## 4. Make a Pull Request

At this point, you should switch back to your master branch and make sure it's
up to date with Parquet.Net master branch:

```sh
git remote add upstream git@github.com:parquet-dotnet.git
git checkout master
git pull upstream master
```

Then update your feature branch from your local copy of master, and push it!

```sh
git checkout 325-add-japanese-translations
git rebase master
git push --set-upstream origin 325-add-japanese-translations
```

Finally, go to GitHub and
[make a Pull Request](https://help.github.com/articles/creating-a-pull-request)
:D

AppVeyor CI will run our test suite against all supported scenarioss. We care
about quality, so your PR won't be merged until all tests pass. It's unlikely,
but it's possible that your changes pass tests on your machine but fail on AppVeyor. In that case you can go ahead and investigate what's wrong as we keep the build logs open.

## 5. Keeping your Pull Request updated

If a maintainer asks you to "rebase" your PR, they're saying that a lot of code
has changed, and that you need to update your branch so it's easier to merge.

To learn more about rebasing in Git, there are a lot of
[good](http://git-scm.com/book/en/Git-Branching-Rebasing)
[resources](https://help.github.com/articles/interactive-rebase),
but here's the suggested workflow:

```sh
git checkout 325-add-japanese-translations
git pull --rebase upstream master
git push --force-with-lease 325-add-japanese-translations
```

## 6. Merging a PR (maintainers only)

A PR can only be merged into master by a maintainer if:

* It is passing CI.
* It has been approved by at least two maintainers. If it was a maintainer who
  opened the PR, only one extra approval is needed.
* It has no requested changes.
* It is up to date with current master.

Any maintainer is allowed to merge a PR if all of these conditions are
met.
