import { useState, useEffect } from 'react'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js'
import { Bar, Line } from 'react-chartjs-2'

// Register Chart.js components
ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
)

// Types for API responses
interface ScoreBucket {
  bucket: string
  count: number
}

interface PassRateItem {
  task: string
  avg_score: number
  attempts: number
}

interface TimelineItem {
  date: string
  submissions: number
}

interface LabItem {
  lab: string
  title: string
}

type FetchState<T> =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; data: T }
  | { status: 'error'; message: string }

const API_BASE_URL = import.meta.env.VITE_API_URL || ''

function Dashboard() {
  const [selectedLab, setSelectedLab] = useState<string>('lab-04')
  const [labs] = useState<LabItem[]>([
    { lab: 'lab-01', title: 'Lab 01' },
    { lab: 'lab-02', title: 'Lab 02' },
    { lab: 'lab-03', title: 'Lab 03' },
    { lab: 'lab-04', title: 'Lab 04' },
  ])

  const [scoresState, setScoresState] = useState<FetchState<ScoreBucket[]>>({
    status: 'idle',
  })
  const [timelineState, setTimelineState] = useState<FetchState<TimelineItem[]>>(
    { status: 'idle' },
  )
  const [passRatesState, setPassRatesState] = useState<FetchState<PassRateItem[]>>(
    { status: 'idle' },
  )

  useEffect(() => {
    const apiKey = localStorage.getItem('api_key')
    if (!apiKey) return

    const headers = { Authorization: `Bearer ${apiKey}` }

    // Fetch scores
    setScoresState({ status: 'loading' })
    fetch(`${API_BASE_URL}/analytics/scores?lab=${selectedLab}`, { headers })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        return res.json()
      })
      .then((data: ScoreBucket[]) =>
        setScoresState({ status: 'success', data }),
      )
      .catch((err: Error) =>
        setScoresState({ status: 'error', message: err.message }),
      )

    // Fetch timeline
    setTimelineState({ status: 'loading' })
    fetch(`${API_BASE_URL}/analytics/timeline?lab=${selectedLab}`, { headers })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        return res.json()
      })
      .then((data: TimelineItem[]) =>
        setTimelineState({ status: 'success', data }),
      )
      .catch((err: Error) =>
        setTimelineState({ status: 'error', message: err.message }),
      )

    // Fetch pass rates
    setPassRatesState({ status: 'loading' })
    fetch(`${API_BASE_URL}/analytics/pass-rates?lab=${selectedLab}`, { headers })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        return res.json()
      })
      .then((data: PassRateItem[]) =>
        setPassRatesState({ status: 'success', data }),
      )
      .catch((err: Error) =>
        setPassRatesState({ status: 'error', message: err.message }),
      )
  }, [selectedLab])

  // Prepare chart data for scores histogram
  const scoresChartData =
    scoresState.status === 'success'
      ? {
          labels: scoresState.data.map((b) => b.bucket),
          datasets: [
            {
              label: 'Number of Students',
              data: scoresState.data.map((b) => b.count),
              backgroundColor: 'rgba(54, 162, 235, 0.6)',
              borderColor: 'rgba(54, 162, 235, 1)',
              borderWidth: 1,
            },
          ],
        }
      : { labels: [], datasets: [] }

  // Prepare chart data for timeline
  const timelineChartData =
    timelineState.status === 'success'
      ? {
          labels: timelineState.data.map((t) => t.date),
          datasets: [
            {
              label: 'Submissions',
              data: timelineState.data.map((t) => t.submissions),
              borderColor: 'rgba(75, 192, 192, 1)',
              backgroundColor: 'rgba(75, 192, 192, 0.2)',
              tension: 0.1,
              fill: true,
            },
          ],
        }
      : { labels: [], datasets: [] }

  return (
    <div className="dashboard">
      <h1>Analytics Dashboard</h1>

      <div className="lab-selector">
        <label htmlFor="lab-select">Select Lab: </label>
        <select
          id="lab-select"
          value={selectedLab}
          onChange={(e) => setSelectedLab(e.target.value)}
        >
          {labs.map((lab) => (
            <option key={lab.lab} value={lab.lab}>
              {lab.title}
            </option>
          ))}
        </select>
      </div>

      <div className="charts-container">
        {/* Scores Histogram */}
        <div className="chart-card">
          <h2>Score Distribution</h2>
          {scoresState.status === 'loading' && <p>Loading...</p>}
          {scoresState.status === 'error' && (
            <p className="error">Error: {scoresState.message}</p>
          )}
          {scoresState.status === 'success' && (
            <Bar
              data={scoresChartData}
              options={{
                responsive: true,
                plugins: {
                  legend: { display: false },
                  title: {
                    display: true,
                    text: 'Scores by Bucket',
                  },
                },
                scales: {
                  y: {
                    beginAtZero: true,
                    ticks: { stepSize: 1 },
                  },
                },
              }}
            />
          )}
        </div>

        {/* Timeline Chart */}
        <div className="chart-card">
          <h2>Submissions Timeline</h2>
          {timelineState.status === 'loading' && <p>Loading...</p>}
          {timelineState.status === 'error' && (
            <p className="error">Error: {timelineState.message}</p>
          )}
          {timelineState.status === 'success' && (
            <Line
              data={timelineChartData}
              options={{
                responsive: true,
                plugins: {
                  legend: { display: false },
                  title: {
                    display: true,
                    text: 'Submissions per Day',
                  },
                },
                scales: {
                  y: {
                    beginAtZero: true,
                    ticks: { stepSize: 1 },
                  },
                },
              }}
            />
          )}
        </div>
      </div>

      {/* Pass Rates Table */}
      <div className="chart-card">
        <h2>Pass Rates by Task</h2>
        {passRatesState.status === 'loading' && <p>Loading...</p>}
        {passRatesState.status === 'error' && (
          <p className="error">Error: {passRatesState.message}</p>
        )}
        {passRatesState.status === 'success' && (
          <table className="pass-rates-table">
            <thead>
              <tr>
                <th>Task</th>
                <th>Average Score</th>
                <th>Attempts</th>
              </tr>
            </thead>
            <tbody>
              {passRatesState.data.map((item) => (
                <tr key={item.task}>
                  <td>{item.task}</td>
                  <td>{item.avg_score.toFixed(1)}</td>
                  <td>{item.attempts}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}

export default Dashboard
