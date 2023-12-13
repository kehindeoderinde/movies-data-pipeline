def test_func (t: list) -> str:
    """Convert tour timestamp to mins"""
    total = 0
    
    for i, ts in enumerate(t):
        if "min" in ts:
            # Handle converting minutes in timestamp
            extracted_min = ts.replace(' ', '').strip("min")
            if extracted_min:
                total += int(extracted_min)
        elif "h" in ts:
            # Handle converting hour in timestamp
            if isinstance(ts, int):
                total += ts * 60
            else:
                total += int(str(ts).strip("h")) * 60
        else:
            # Handle case for when there's mistype integer as string such as 21S instead of 215
            # This also handles case when time type 'min' or 'h' is separated from numeric value by a trailing space
            if ts.isnumeric():
                if t[i+1] == "min":
                    total += int(ts) 
                elif t[i+1] == 'h':
                    total += int(ts) * 60
            else:
                total += 0
                   
    return total


print(test_func('1h 30 min'.split(' ')))

print(test_func('75min'.split(' ')))

print(test_func('21S min'.split(' ')))

print(test_func('2h 75 min'.split(' ')))

