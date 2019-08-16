# Contributing to Angel
Welcome to [report Issues](https://github.com/Tencent/angel/issues) or [pull requests](https://github.com/Tencent/angel/pulls). It's recommended to read the following Contributing Guide first before contributing. 


## Reporting Bugs and Feature Requests
We use Github Issues to track bug reports and feature requests.

### Search Known Issues First
If you have a bug to report or intend to add a new feature to Angel, please search the existing issues to see if any similar issue or feature request has already been filed. You should make sure your issue isn't redundant prior to opening it.

### Reporting New Issues
After ensuring the bug or feature was not already reported, you can [open a new one](<https://github.com/Angel-ML/angel/issues/new>). The more information the better. Please be sure to provide as much information as possible, such as detailed description, screenshot or video of your problem, logcat or code blocks for your crash.

## Pull Requests
We strongly welcome your pull request to make Angel better. 

Before opening a Pull Request, please make sure you have signed the [Contributor License Agreement (CLA).](master/CLA.md)


### Branch Management
There are three main branches here:

1. `master` branch.

	(1). It is the latest (pre-)release branch. We use `master` for tags, with version number `1.0.0`, `1.1.0`, `1.2.0`...

	(2). **Don't submit any PR on `master` branch.**
	
2. `specific version` branchs. 

	(1).There is a `specific version` for each Angel version, such as `branch-1.0.0`, `branch-1.1.0`. It is our stable developing	 branch. After full testing, `specific version` branch will be merged to `master` branch for the next release.

	(2). **You are recommended to submit bugfix or feature PR on `specific version` branch.**


Normal bugfix or feature request should be submitted to `specific version` branch. After full testing, we will merge them to `master` branch for the next release. 


### Open a Pull Request
The code team will monitor all pull request, we run some code check and test on it. After all tests passed, we will accecpt this PR. But it won't merge to `master` branch at once, which have some delay.

Before submitting a pull request, please make sure the followings are done:

1. Fork the repo and create your branch from `master` or `specific version`.
2. Update code or documentation if you have changed APIs.
3. Add the copyright notice to the top of any new files you've added.
4. Check your code lints and checkstyles.
5. Test and test again your code.
6. Now, you can submit your pull request on  `specific version` branch.

## Code Style Guide
A [Checkstyle configuration](https://github.com/Tencent/angel/blob/master/dev/checkstyle.xml) is provided along with Angel. We STRONGLY suggest that all code contributions follow the Google Code Style for Java and Scala. 

## License
By contributing to Angel, you accept and agree that your contributions will be licensed
under its [ Apache License, Version 2.0](https://github.com/Tencent/angel/blob/master/LICENSE)

