if __name__ == "__main__":
    import requests
    import time
    from datetime import datetime

    POLLING_DELAY_IN_SECONDS = 2
    EXTENDING_WALLTIME_DELAY_IN_SECONDS = 600
    WALLTIME_EXTENSION_IN_SECONDS = 3600
    WALLTIME_EXTENSION_FORMATTED = "+01:00:00"


    def get_running_jobs(site: str, user: str):
        req = requests.get(url=f"https://api.grid5000.fr/stable/sites/{site}/jobs?user={user}&state=running",
                           verify="/etc/ssl/certs/ca-certificates.crt")

        if req.status_code == 200:
            return req.json()["items"]


    def get_job_deadlines(jobs: list[dict]):
        deadlines = {}
        for job in jobs:
            uid = str(job["uid"])
            deadlines[uid] = job["started_at"] + job["walltime"]

        return deadlines


    def extend_walltime(site: str, job: str, walltime: str):
        req = requests.post(url=f"https://api.grid5000.fr/stable/sites/{site}/jobs/{job}/walltime",
                            data={"walltime": walltime},
                            verify="/etc/ssl/certs/ca-certificates.crt")

        if req.status_code == 200 or req.status_code == 202:
            return req.json()


    jobs = get_running_jobs("nancy", "adugois")
    deadlines = get_job_deadlines(jobs)

    while True:
        now_in_seconds = datetime.now().timestamp()
        for job in jobs:
            uid = str(job["uid"])
            if deadlines[uid] - now_in_seconds < EXTENDING_WALLTIME_DELAY_IN_SECONDS:
                deadlines[uid] += WALLTIME_EXTENSION_IN_SECONDS
                print(f"Extending walltime of job {uid} to {deadlines[uid]}.")
                res = extend_walltime("nancy", uid, WALLTIME_EXTENSION_FORMATTED)
                print(res)

        time.sleep(POLLING_DELAY_IN_SECONDS)
