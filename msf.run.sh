#!/bin/bash

set -eo pipefail

if [ -z "$1" ]
  then
    echo "Usage: msf.run.sh <fork name> (<fork name 2> ...)"
    echo " for each fork, there must be a 'calculate_average_<fork name>.sh' script and an optional 'prepare_<fork name>.sh'."
    exit 1
fi

BOLD_WHITE='\033[1;37m'
CYAN='\033[0;36m'
GREEN='\033[0;32m'
PURPLE='\033[0;35m'
BOLD_RED='\033[1;31m'
RED='\033[0;31m'
BOLD_YELLOW='\033[1;33m'
RESET='\033[0m' # No Color

MEASUREMENTS_FILE="measurements_1B.txt"
RUNS=10
DEFAULT_JAVA_VERSION="21.0.1-open"
: "${BUILD_JAVA_VERSION:=21.0.1-open}"
RUN_TIME_LIMIT=300 # seconds
TIMEOUT="timeout -v $RUN_TIME_LIMIT"

function check_command_installed {
  if ! [ -x "$(command -v $1)" ]; then
    echo "Error: $1 is not installed." >&2
    exit 1
  fi
}

function print_and_execute() {
  echo "+ $@" >&2
  "$@"
}

check_command_installed hyperfine
check_command_installed jq
check_command_installed bc
check_command_installed numactl
check_command_installed hyperfine

# Validate that ./calculate_average_<fork>.sh exists for each fork
for fork in "$@"; do
  if [ ! -f "./calculate_average_$fork.sh" ]; then
    echo -e "${BOLD_RED}ERROR${RESET}: ./calculate_average_$fork.sh does not exist." >&2
    exit 1
  fi
done

# Check if SMT is enabled (we want it disabled)
if [ -f "/sys/devices/system/cpu/smt/active" ]; then
  if [ "$(cat /sys/devices/system/cpu/smt/active)" != "0" ]; then
    echo -e "${BOLD_YELLOW}WARNING${RESET} SMT is enabled"
  fi
fi

# Check if Turbo Boost is enabled (we want it disabled)
if [ -f "/sys/devices/system/cpu/cpufreq/boost" ]; then
  if [ "$(cat /sys/devices/system/cpu/cpufreq/boost)" != "0" ]; then
    echo -e "${BOLD_YELLOW}WARNING${RESET} Turbo Boost is enabled"
  fi
fi

# Run tests and benchmark for each fork
filetimestamp=$(date  +"%Y%m%d%H%M%S") # same for all fork.out files from this run
failed=()
for fork in "$@"; do
  set +e # we don't want prepare.sh, test.sh or hyperfine failing on 1 fork to exit the script early

  # Run prepare script
  if [ -f "./prepare_$fork.sh" ]; then
    print_and_execute source "./prepare_$fork.sh"
  fi

  # Use hyperfine to run the benchmark for each fork
  HYPERFINE_OPTS="--warmup 1 --runs $RUNS --export-json $fork-$filetimestamp-timing.json --output ./$fork-$filetimestamp.out"

  hyperfine $HYPERFINE_OPTS "$TIMEOUT ./calculate_average_$fork.sh 2>&1"
  # Catch hyperfine command failed
  if [ $? -ne 0 ]; then
    failed+=("$fork")
    # Hyperfine already prints the error message
    echo ""
    continue
  fi
done
set -e

# Summary
echo -e "${BOLD_WHITE}Summary${RESET}"
for fork in "$@"; do
  # skip reporting results for failed forks
  if [[ " ${failed[@]} " =~ " ${fork} " ]]; then
    echo -e "  ${RED}$fork${RESET}: command failed or output did not match"
    continue
  fi

  # Trimmed mean = The slowest and the fastest runs are discarded, the
  # mean value of the remaining three runs is the result for that contender
  trimmed_mean=$(jq -r '.results[0].times | sort_by(.|tonumber) | .[1:-1] | add / length' $fork-$filetimestamp-timing.json)
  raw_times=$(jq -r '.results[0].times | join(",")' $fork-$filetimestamp-timing.json)

  if [ "$fork" == "$1" ]; then
    color=$CYAN
  elif [ "$fork" == "$2" ]; then
    color=$GREEN
  else
    color=$PURPLE
  fi

  echo -e "  ${color}$fork${RESET}: trimmed mean ${BOLD_WHITE}$trimmed_mean${RESET}, raw times ${BOLD_WHITE}$raw_times${RESET}"
done
echo ""
