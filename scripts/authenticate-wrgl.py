#!/usr/bin/env python
import argparse

from wrgl import Repository

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="authenticate-wrgl",
        description="exchanges Keycloak credentials for access token",
    )
    parser.add_argument("repo_uri")
    parser.add_argument("client_id")
    parser.add_argument("client_secret")
    args = parser.parse_args()
    repo = Repository(args.repo_uri, args.client_id, args.client_secret)
    rpt = repo.authenticate()
    print(rpt)
