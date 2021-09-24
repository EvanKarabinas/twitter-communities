def format(start, end):
    elapsed_time = end-start
    # if(elapsed_time < 60):
    #     return f"\nRun time : {elapsed_time} s."
    # if(elapsed_time < 60*60):
    #     return f"\nRun time : {elapsed_time//60} min , {elapsed_time%60} s."

    # return f"\nRun time : {elapsed_time//(60*60)} h , {elapsed_time%(60*60)} min."
    return f"\nRun time : {round(elapsed_time/60)} min ."
