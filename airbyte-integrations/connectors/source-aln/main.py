#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_aln import SourceAln

if __name__ == "__main__":
    source = SourceAln()
    launch(source, sys.argv[1:])
